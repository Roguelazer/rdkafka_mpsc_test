extern crate rdkafka;

use std::thread;
use std::time::Duration;
use std::sync::mpsc::{Sender,channel};
use std::sync::{Arc, Mutex};

use rdkafka::config::FromClientConfigAndContext;

struct DeliveryContext {
    message_id: usize
}

struct ProducerContext {
    tx: Arc<Mutex<Sender<usize>>>
}

impl ProducerContext {
    fn new(tx: Sender<usize>) -> Self {
        ProducerContext {
            tx: Arc::new(Mutex::new(tx))
        }
    }
}

impl rdkafka::client::Context for ProducerContext {
}

impl rdkafka::producer::ProducerContext for ProducerContext {
    type DeliveryContext = DeliveryContext;

    fn delivery(&self, report: rdkafka::producer::DeliveryReport, ctx: Self::DeliveryContext) -> () {
        if let Ok(tx) = self.tx.lock() {
            tx.send(ctx.message_id);
        }
    }
}

fn main() {
    let (tx, rx) = channel();
    let worker = thread::spawn(move|| {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config.set("bootstrap.servers", "localhost:9092");
        let topic_config = rdkafka::config::TopicConfig::new();
        let context = ProducerContext::new(tx);
        let conn = rdkafka::producer::BaseProducer::from_config_and_context(&client_config, context).unwrap();
        let mut message_id = 0;
        loop {
            conn.poll(1);
            let topic = conn.get_topic("segfault", &topic_config).unwrap();
            println!("sending copy");
            topic.send_copy(None, Some(&"foobar"), Some(&"key"), Some(Box::new(DeliveryContext { message_id: message_id }))).unwrap();
            message_id += 1;
            thread::sleep(Duration::from_millis(100));
        }
    });
    while let Ok(result) = rx.recv() {
        println!("finished {:?}", result);
    }
    println!("rx failed!");
    worker.join();
}
