import org.apache.kafka.clients.producer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;

public class KafkaProducerTest implements Runnable {
  private final KafkaProducer<String, String> producer;


  public KafkaProducerTest(Properties config) {
    this.producer = new KafkaProducer<>(config);
  }

  public void run() {
    for (int i = 0; i < 100; i++) {
      producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));
      System.out.println("Sending " + i);
    }
    producer.close();
  }

  public static void main(String [] args) {
    Properties config = new Properties();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("acks", "all");
    config.put("retries", 100);
    config.put("batch.size", 16384);
    config.put("linger.ms", 1);
    config.put("buffer.memory", 33554432);
    config.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");  
    config.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducerTest test = new KafkaProducerTest(config);
    test.run();
  }
}