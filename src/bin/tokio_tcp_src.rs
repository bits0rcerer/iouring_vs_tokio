use std::{env::args, str::FromStr, sync::Arc};

use rand::RngCore;
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let target = args().nth(2).unwrap_or("127.0.0.1:1234".to_string());
    let connections = args().nth(1).unwrap_or("1".to_string());
    let connections =
        usize::from_str(&connections).expect("2. parameter to be the number of connections");

    let mut data = vec![0u8; 128 * 1024 * 1024];
    rand::thread_rng().fill_bytes(&mut data);
    let data = Arc::new(data.into_boxed_slice());

    let mut handles = vec![];
    for _ in 0..connections {
        let mut conn = TcpStream::connect(target.as_str()).await?;
        let data = data.clone();

        handles.push(tokio::spawn(async move {
            loop {
                if let Err(e) = conn.write_all(&data).await {
                    eprintln!("unable to send data: {e}");
                    break;
                }
            }
        }));
    }

    for h in handles {
        h.await?
    }

    Ok(())
}
