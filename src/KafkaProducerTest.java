import org.apache.kafka.clients.producer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.sql.ResultSet;
import java.lang.Runtime;

public class KafkaProducerTest implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final DBConnectorTest connector;
    private int last_version;

    public KafkaProducerTest(Properties config, String ip) {
        //this.producer = null;
        this.producer = new KafkaProducer<>(config);
        this.connector = new DBConnectorTest(ip);
        //Doesn't work yet, will work on it later
        /*Runtime.getRuntime.addShutdownHook(new Thread(){
            public void run() {
                try {
                    Thread.sleep(200);
                    producer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });*/
    }

    public void run() {
        try {
            //Set up initial refresh query. We should add configs for this at a later point
            //String sql = "SELECT person_uid, add_reason_cd FROM NBS_ODSE.dbo.Person";
            String sql = "SELECT * FROM NBS_ODSE.dbo.Entity";
            connector.query(sql);
            ResultSet result = connector.getResults();
            //Parse results from intial query

            while (result.next()) {
                String row = String.format("%d %s", result.getInt(1), result.getString(2));
                System.out.println(row);
                producer.send(new ProducerRecord<String, String>("sql-jdbc-tables-Person", "I", row));
            }


            //Monitor loop
            while (true) {
                //TODO We need to change the below query to selecting based on last_chg_time
                //sql = String.format("SELECT SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION, person_uid FROM CHANGETABLE (CHANGES NBS_ODSE.dbo.Person, %d) AS C", last_version);
                sql = String.format("SELECT SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION, entity_uid FROM CHANGETABLE (CHANGES NBS_ODSE.dbo.Entity, %d) AS C", last_version);
                connector.query(sql);
                result = connector.getResults();
                //Parse results from monitor query
                while (result.next()) {
                    last_version = result.getInt(1);
                    String op = result.getString(2);
                    String person_uid = result.getString(3);

                    sql = String.format("SELECT * FROM NBS_ODSE.dbo.Entity WHERE entity_uid=%s", person_uid);
                    connector.query(sql);
                    ResultSet result2 = connector.getResults();
                    while (result2.next()) {

                        producer.send(new ProducerRecord<String, String>("test1", op, String.format("%s %s", result2.getString(1), result2.getString(2))));
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

        KafkaProducerTest test = new KafkaProducerTest(config, args[0]);
        test.run();
    }

}