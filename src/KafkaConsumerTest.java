import org.apache.kafka.clients.consumer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.sql.ResultSet;
import java.lang.Runtime;

import org.json.*;

public class KafkaConsumerTest implements Runnable {
  private final KafkaConsumer<String, String> consumer;
  private final List<String> topics;
  private final AtomicBoolean shutdown;
  private final CountDownLatch shutdownLatch;
  private final HashMap<String, String> ops;
  private final ODSConnector odsConnector;
  private final RDBConnector rdbConnector;

  public KafkaConsumerTest(Properties config, List<String> topics, String ip) {
    this.consumer = new KafkaConsumer<>(config);
    this.topics = topics;
    this.shutdown = new AtomicBoolean(false);
    this.shutdownLatch = new CountDownLatch(1);
    this.odsConnector = new ODSConnector(ip);
    this.rdbConnector = new RDBConnector(ip);

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
    } else if (topic.equals("sql-jdbc-tables-Postal_locator")) {
    	process_postal(record.value());
    } else {
    	System.out.println("Record is not from Person or Postal Locator");
    }
  }

  public void process_person(String rawjson) {
  	JSONObject json = null;
    try {
    	json = new JSONObject(rawjson).getJSONObject("payload");
      System.out.println(json.get("person_uid"));
      long person_uid = json.getInt("person_uid");
      String person_first = json.getString("first_nm");
      String person_last = json.getString("last_nm");
      String sql = String.format("update RDB.dbo.S_PATIENT "
                               + "set PATIENT_FIRST_NAME = '%s', "
                               + "PATIENT_LAST_NAME = '%s' "
                               + "where PATIENT_UID = %d;", 
                               person_first, person_last, person_uid);
      rdbConnector.query(sql);
    } catch(JSONException e) {
		e.printStackTrace();
	}
  }

  public void process_postal(String rawjson) {
    JSONObject json = null;
    try {
    	json = new JSONObject(rawjson).getJSONObject("payload");
    	int postal_locator_uid = json.getInt("postal_locator_uid");
      String city = (json.isNull("city_cd")) ? null : json.getString("city_cd");
      String state = (json.isNull("state_cd")) ? null : json.getString("state_cd");
      String street_addr1 = (json.isNull("street_addr1")) ? null : json.getString("street_addr1");
      String street_addr2 = (json.isNull("street_addr2")) ? null : json.getString("street_addr2");
	    System.out.println(String.format("%s : %s : %s : %s : %d", city, state, street_addr1, street_addr2, postal_locator_uid));
	    String sql = "select entity_uid from NBS_ODSE.dbo.Entity_locator_participation where locator_uid = " + postal_locator_uid;
	    odsConnector.query(sql);
      ResultSet result = odsConnector.getResults();
      int entity_uid = 0;
      while (result.next()) {
        entity_uid = result.getInt(1);
        System.out.println(entity_uid);
      }
      sql = String.format("update RDB.dbo.S_PATIENT "
                        + "set PATIENT_CITY = '%s', "
                        + "PATIENT_STATE = '%s', "
                        + "PATIENT_STREET_ADDRESS_1 = '%s', "
                        + "PATIENT_STREET_ADDRESS_2 = '%s' "
                        + "where PATIENT_UID = %d;", 
                        city, state, street_addr1, street_addr2, entity_uid);
      rdbConnector.query(sql);

    } catch(JSONException e) {
		  e.printStackTrace();
	  } catch(Exception e) {
      // e.printStackTrace();
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
    alist.add("sql-jdbc-tables-Postal_locator");

    KafkaConsumerTest test = new KafkaConsumerTest(config, alist, "128.61.21.133");
    test.run();
  }
}