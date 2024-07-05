use std::{
    env::args,
    mem::MaybeUninit,
    net::{TcpListener, TcpStream},
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
    ptr::slice_from_raw_parts_mut,
    time::Duration,
};

use io_uring::{cqueue, opcode, squeue, types::Fd};

const ACCEPT: u64 = u64::MAX;
const POLL: u64 = u64::MAX - 1;

fn main() -> eyre::Result<()> {
    println!("iouring tcp sink");

    let threads = args().nth(1).unwrap_or("4".to_string());
    let threads = usize::from_str_radix(&threads, 10)?;

    let socket = TcpListener::bind("[::0]:1234")?;
    let socket_fd = socket.as_raw_fd();

    let handles = (0..threads)
        .map(|_| std::thread::spawn(move || ring_main(socket_fd)))
        .collect::<Vec<_>>();

    for h in handles {
        h.join().expect("thread join")?;
    }

    Ok(())
}

fn ring_main(listen_fd: i32) -> eyre::Result<()> {
    // setup io uring
    let mut ring: io_uring::IoUring<squeue::Entry, cqueue::Entry> = io_uring::IoUring::builder()
        .setup_defer_taskrun()
        .setup_coop_taskrun()
        .setup_single_issuer()
        .build(128)?;

    let (submitter, mut sq, mut cq) = ring.split();

    // setup buffer ring for registered buffers
    let mut buf_ring = submitter.setup_buf_ring(256, 42)?;
    let buf_len = 1 * 1024 * 1024;
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
                libc::madvise(addr, buf_len * buf_ring.capacity(), libc::MADV_SEQUENTIAL);
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

    // Using poll instead of multishot accept because this distributes new connection
    // accross all threads.
    //
    // Using multishot accept on all threads on a single socket did only create cqes
    // on a single thread.
    //
    // (at least on my machine :])
    unsafe {
        let poll = opcode::PollAdd::new(Fd(listen_fd), libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(POLL);
        sq.push(&poll).expect("sq is empty");
    }

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
                POLL => match cqe.result() {
                    0 => continue,
                    e if e < 0 => {
                        panic!("unable to poll: {}", std::io::Error::from_raw_os_error(-e))
                    }
                    _ => unsafe {
                        let mut addr = std::mem::zeroed();
                        let mut addr_len = std::mem::zeroed();
                        let accept = opcode::Accept::new(
                            Fd(listen_fd),
                            std::ptr::addr_of_mut!(addr),
                            std::ptr::addr_of_mut!(addr_len),
                        )
                        .build()
                        .user_data(ACCEPT);

                        sq.push(&accept).expect("sq not full");

                        if !cqueue::more(cqe.flags()) && cqe.result() > 0 {
                            let poll = opcode::PollAdd::new(Fd(listen_fd), libc::POLLIN as u32)
                                .multi(true)
                                .build()
                                .user_data(POLL);
                            sq.push(&poll).expect("sq is empty");
                        }
                    },
                },
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
                        n => {
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
