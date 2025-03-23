use std::{
    env::args,
    mem::MaybeUninit,
    net::{SocketAddr, TcpStream},
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
    ptr::slice_from_raw_parts_mut,
    str::FromStr,
    time::Duration,
};

use io_uring::{cqueue, opcode, squeue, types::Fd};
use socket2::{Domain, Type};

const ACCEPT: u64 = u64::MAX;

fn main() -> eyre::Result<()> {
    println!("iouring tcp sink");

    let threads = args().nth(1).unwrap_or("4".to_string());
    let threads = threads.parse()?;

    let mut rings: Vec<io_uring::IoUring> = vec![];
    let mut handles = vec![];

    for _ in 0..threads {
        let mut ring_builder = io_uring::IoUring::<squeue::Entry, cqueue::Entry>::builder();
        ring_builder
            .setup_r_disabled()
            .setup_single_issuer()
            .setup_coop_taskrun()
            .setup_taskrun_flag()
            .setup_defer_taskrun();

        if let Some(ring) = rings.first() {
            ring_builder.setup_attach_wq(ring.as_raw_fd());
        }

        rings.push(ring_builder.build(1024)?);
    }
    for ring in rings {
        let socket = socket2::Socket::new(Domain::IPV6, Type::STREAM, None)?;
        socket.set_reuse_port(true)?;

        let addr = SocketAddr::from_str("[::0]:1234")?.into();
        socket.bind(&addr)?;
        socket.listen(128)?;

        handles.push(std::thread::spawn(move || ring_main(socket, ring)));
    }

    for h in handles {
        h.join().expect("thread join")?;
    }

    Ok(())
}

fn ring_main(listen_socket: socket2::Socket, mut ring: io_uring::IoUring) -> eyre::Result<()> {
    let (submitter, mut sq, mut cq) = ring.split();
    submitter.register_enable_rings()?;
    let mut workers = [0, 0];
    submitter.register_iowq_max_workers(&mut workers)?;
    println!("workers: {workers:?}");

    // setup buffer ring for registered buffers
    let mut buf_ring = submitter.setup_buf_ring(256, 42)?;
    let buf_len = 1024 * 1024;
    let bufs = unsafe {
        let bufs = match libc::mmap(
            std::ptr::null_mut(),
            buf_len * buf_ring.capacity(),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_POPULATE,
            -1,
            0,
        ) {
            libc::MAP_FAILED => {
                panic!("unable to map buffers: {}", std::io::Error::last_os_error())
            }
            addr => {
                libc::madvise(
                    addr,
                    buf_len * buf_ring.capacity(),
                    libc::MADV_SEQUENTIAL | libc::MADV_HUGEPAGE,
                );
                std::ptr::NonNull::new_unchecked(addr).cast::<MaybeUninit<u8>>()
            }
        };

        let bufs_iter = (0..buf_ring.capacity()).map(|id| {
            (
                id as u16,
                &mut *slice_from_raw_parts_mut(bufs.add(id * buf_len).as_ptr(), buf_len),
            )
        });
        buf_ring.push_multiple(bufs_iter);

        bufs
    };

    accept(&listen_socket, &mut sq);

    loop {
        sq.sync();
        loop {
            match submitter.submit_and_wait(1) {
                Ok(_) => break,
                Err(e) => match e.raw_os_error().unwrap_or(0) {
                    0 => panic!("unknown ring error"),
                    libc::EAGAIN => std::thread::sleep(Duration::from_millis(100)),
                    libc::EBUSY => break,
                    libc::EINTR => continue,
                    _ => panic!("ring error: {e}"),
                },
            };
        }

        cq.sync();
        for cqe in &mut cq {
            match cqe.user_data() {
                0 => continue,
                ACCEPT => {
                    let fd = match cqe.result() {
                        e if e < 0 => panic!(
                            "unable to accept connection: {}",
                            std::io::Error::from_raw_os_error(-e)
                        ),
                        fd => fd,
                    };

                    let s = unsafe { TcpStream::from_raw_fd(fd) };
                    println!(
                        "[{:?}] + {} {}",
                        std::thread::current().id(),
                        fd,
                        s.peer_addr().unwrap()
                    );

                    let recv = opcode::RecvMulti::new(Fd(s.as_raw_fd()), buf_ring.bgid())
                        .build()
                        .user_data(s.into_raw_fd() as u64);
                    unsafe {
                        sq.push(&recv).expect("sq not full");
                    }

                    if !cqueue::more(cqe.flags()) {
                        accept(&listen_socket, &mut sq);
                    }
                }
                stream_fd => {
                    match cqe.result() {
                        0 => {
                            let _s = unsafe { TcpStream::from_raw_fd(stream_fd as i32) };
                            println!("[{:?}] - {}", std::thread::current().id(), stream_fd);
                        }
                        e if e < 0 => eprintln!(
                            "unable to read from connection: {}",
                            std::io::Error::from_raw_os_error(-e)
                        ),
                        _n => {
                            //println!("recv {n} bytes");
                            // magic
                        }
                    }

                    if let Some(buf_id) = cqueue::buffer_select(cqe.flags()) {
                        //println!("buf_id: {buf_id}");
                        unsafe {
                            buf_ring.push(
                                buf_id,
                                &mut *slice_from_raw_parts_mut(
                                    bufs.add(buf_id as usize * buf_len).as_ptr(),
                                    buf_len,
                                ),
                            )
                        };
                    }

                    if !cqueue::more(cqe.flags()) && cqe.result() > 0 {
                        let recv = opcode::RecvMulti::new(Fd(stream_fd as i32), buf_ring.bgid())
                            .build()
                            .user_data(stream_fd);
                        unsafe {
                            sq.push(&recv).expect("sq not full");
                        }
                    }
                }
            }
        }
    }

    // unsafe {
    //     libc::munmap(bufs, buf_len * buf_ring.capacity());
    // }

    // Ok(())
}

fn accept(listen_socket: &socket2::Socket, sq: &mut squeue::SubmissionQueue) {
    unsafe {
        let accept = opcode::AcceptMulti::new(Fd(listen_socket.as_raw_fd()))
            .build()
            .user_data(ACCEPT);
        sq.push(&accept).expect("sq is empty");
    }
}
