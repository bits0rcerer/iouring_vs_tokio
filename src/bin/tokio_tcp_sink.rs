use std::{error::Error, time::Instant};

use tokio::{io::AsyncReadExt, net::TcpListener};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Tokio tcp sink");

    let socket = TcpListener::bind("[::0]:1234").await?;
    while let Ok((mut stream, addr)) = socket.accept().await {
        println!("+ {addr}");

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4 * 1024 * 1024];
            let start = Instant::now();
            let mut bytes = 0;

            while let Ok(arst) = stream.read(&mut buf).await {
                if arst == 0 {
                    break;
                }
                bytes += arst as u128;
            }

            let time = start.elapsed();
            let speed = 8 * (bytes / 1024 / 1024) / time.as_secs() as u128;
            let speed = speed as f64 / 1024.0;
            let mbytes = bytes / 1024 / 1024;
            println!(
                "- {addr} ({speed:.2} GBit/s, {:.2} GByte)",
                mbytes as f64 / 1024.0
            );
        });
    }

    Ok(())
}
