import org.apache.kafka.clients.consumer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;

import org.json.*;

public class KafkaConsumerTest implements Runnable {
  private final KafkaConsumer<String, String> consumer;
  private final List<String> topics;
  private final AtomicBoolean shutdown;
  private final CountDownLatch shutdownLatch;
  private final HashMap<String, String> ops;

  public KafkaConsumerTest(Properties config, List<String> topics) {
    this.consumer = new KafkaConsumer<>(config);
    this.topics = topics;
    this.shutdown = new AtomicBoolean(false);
    this.shutdownLatch = new CountDownLatch(1);

    ops = new HashMap<>();
    ops.put("I", "Insert ");
    ops.put("U", "Update ");
    ops.put("D", "Delete ");
  }

  public void process(ConsumerRecord<String, String> record) {
  	String topic = record.topic();
  	System.out.println("Topic");
  	System.out.println(topic);
    if (topic.equals("sql-jdbc-tables-Person")) {
    	process_person(record.value());
    } else if (topic.equals("sql-jdbc-tables-Participation")) {
    	process_participation(record.value());
    } else {
    	System.out.println("Record is not from Person or Participation");
    }
  }

  public void process_person(String rawjson) {
  	JSONObject json = null;
    try {
    	json = new JSONObject(rawjson).getJSONObject("payload");
	    System.out.println(json.get("person_uid"));
    } catch(JSONException e) {
		e.printStackTrace();
	}
  }

  public void process_participation(String rawjson) {
    JSONObject json = null;
    try {
    	json = new JSONObject(rawjson).getJSONObject("payload");
	    System.out.println(json.get("subject_entity_uid"));
    } catch(JSONException e) {
		e.printStackTrace();
	}
  }

  public void run() {
    try {
      consumer.subscribe(topics);

      while (!shutdown.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        records.forEach(record -> process(record));
      }
    } catch (Exception e) {

    } finally {
      consumer.close();
      shutdownLatch.countDown();
    }
  }

  public void shutdown() throws InterruptedException {
    shutdown.set(true);
    shutdownLatch.await();
  }

  public static void main(String [] args) {
    Properties config = new Properties();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "foo");

    ArrayList<String> alist = new ArrayList<>();
    alist.add("sql-jdbc-tables-Person");
    alist.add("sql-jdbc-tables-Participation");

    KafkaConsumerTest test = new KafkaConsumerTest(config, alist);
    test.run();
  }
}