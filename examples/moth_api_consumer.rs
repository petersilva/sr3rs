use anyhow::Result;
use sr3rs::broker::Broker;
use sr3rs::moth::MothFactory;
use tokio::time::{sleep, Duration};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup broker
    let broker_url = "amqps://anonymous:anonymous@hpfx.collab.science.gc.ca/";
    let mut broker = Broker::parse(broker_url)?;
    
    println!("Connecting to broker: {}", broker_url);

    // 2. Create Moth instance (is_subscriber = true)
    let mut moth = MothFactory::new(&broker, true).await?;

    // 3. Subscribe to topics
    let topics = vec!["v02.post.#".to_string()];
    let exchange = "xpublic";
    
    // Random queue name to avoid collision
    let mut rng = rand::thread_rng();
    let rand_id: u32 = rng.gen();
    let queue_name = format!("q_anonymous_rust_example_{:x}", rand_id);
    
    println!("Subscribing to {:?} on exchange {} with queue {}", topics, exchange, queue_name);
    
    // In Moth, subscribe() also declares queue and binds it.
    moth.subscribe(&topics, exchange, &queue_name).await?;

    // 4. Consume messages
    let mut count = 0;
    while count < 5 {
        // Use a timeout to avoid hanging forever if no messages arrive
        match tokio::time::timeout(Duration::from_secs(10), moth.consume()).await {
            Ok(res) => {
                match res {
                    Ok(Some(msg)) => {
                        println!("Received message: {}/{}", msg.base_url, msg.rel_path);
                        
                        // Ack the message if it has an ack_id
                        if let Some(ack_id) = &msg.ack_id {
                            println!("Acknowledging message with ack_id: {}", ack_id);
                            moth.ack(ack_id).await?;
                        }
                        
                        count += 1;
                    }
                    Ok(None) => {
                        println!("No more messages (stream closed).");
                        break;
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {}", e);
                        break;
                    }
                }
            }
            Err(_) => {
                println!("Timeout waiting for message, retrying...");
            }
        }
        
        sleep(Duration::from_millis(100)).await;
    }

    println!("Got {} messages. Cleaning up...", count);

    // 5. Cleanup (delete queue)
    if let Err(e) = moth.delete_queue(&queue_name).await {
        eprintln!("Warning: Failed to delete queue {}: {}", queue_name, e);
    }

    // 6. Close connection
    moth.close().await?;
    
    println!("Done.");
    Ok(())
}
