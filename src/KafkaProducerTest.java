import org.apache.kafka.clients.producer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.sql.ResultSet;

public class KafkaProducerTest implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final DBConnectorTest connector;

    public KafkaProducerTest(Properties config, String ip) {
        this.producer = new KafkaProducer<>(config);
        this.connector = new DBConnectorTest(ip);
    }

    public void run() {
    // String sql = "SELECT person_uid, add_reason_cd FROM NBS_ODSE.dbo.Person";
        while (true) {
            String sql = "SELECT * FROM CHANGETABLE (CHANGES NBS_ODSE.dbo.Person, NULL) AS C";
            connector.query(sql);
            ResultSet result = connector.getResults();
            try {
                while (result.next()) {
                    String result_str = result.getInt(1) + " " + result.getString(3) + " " + result.getString(6);
                    System.out.println(result_str);
                    producer.send(new ProducerRecord<String, String>("test1", Integer.toString(0), result_str));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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