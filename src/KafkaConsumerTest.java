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
  private final Connector connector;

  public KafkaConsumerTest(Properties config, List<String> topics, String ip) {
    this.consumer = new KafkaConsumer<>(config);
    this.topics = topics;
    this.shutdown = new AtomicBoolean(false);
    this.shutdownLatch = new CountDownLatch(1);
    this.connector = new Connector(ip);

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
    } else if (topic.equals("sql-jdbc-tables-Person_race")) {
      process_race(record.value());
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
      connector.query(sql);
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
	    connector.query(sql);
      ResultSet result = connector.getResults();
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
      connector.query(sql);

    } catch(JSONException e) {
		  e.printStackTrace();
	  } catch(Exception e) {
      // e.printStackTrace();
    }
  }

  public void process_race(String rawjson) {
    JSONObject json = null;
    String sql = "";
    ResultSet result;
    try {
      json = new JSONObject(rawjson).getJSONObject("payload");
      int person_uid = json.getInt("person_uid");
      //calculate the PATIENT_RACE_CALCULATED
      //instantiate list of races and categories
      ArrayList<String> races = new ArrayList<>();
      sql = "select race_cd from NBS_ODSE.dbo.Person_race where person_uid = " + person_uid;
      connector.query(sql);
      result = connector.getResults();
      String race = "";
      while (result.next()) {
        race = result.getString(1);
        races.add(race);
      }
      String[] decoded_races = new String[races.size()];
      for (int i = 0; i < races.size(); i++) {
        sql = "select code_desc_txt from NBS_SRTE.dbo.Race_code where code = " + r;
        connector.getResults();
        decoded_races[i] = result.getString(1);
      }
      String race_calculated = decoded_races[0];
      String race_calculated_details = "";
      for (String r : decoded_races) {
        race_calculated_details = race_calculated_details + r + " | ";
      }
      if (decoded_races.length > 1) {
        race_calculated = "Multi-Race";
      }

      // Update RDB
      System.out.println(String.format("Calculated Race: %s \n Calculated Race Details: %s", race_calculated, race_calculated_details));
      String sql = String.format("update RDB.dbo.S_PATIENT "
                                + "set PATIENT_RACE_CALCULATED = '%s', "
                                + "PATIENT_RACE_CALC_DETAILS ='%s', "
                                + "where person_uid = %d",
                                race_calculated, race_calculated_details, person_uid);
      connector.query(sql);
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
    alist.add("sql-jdbc-tables-Person_race");

    KafkaConsumerTest test = new KafkaConsumerTest(config, alist, "128.61.21.133");
    test.run();
  }
}