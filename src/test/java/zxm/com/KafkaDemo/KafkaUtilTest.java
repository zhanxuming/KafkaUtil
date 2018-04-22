package zxm.com.KafkaDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.zxm.util.KafkaUtil;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class KafkaUtilTest extends TestCase {

	public KafkaUtilTest(String name) {
		super(name);
	}

	public static Test suite() {
		return new TestSuite(KafkaUtilTest.class);
	}

	public void testCreate() {

		/*Properties properties = new Properties();
		properties.put("bootstrap.servers", "hadoop11:9092,hadoop12:9092,hadoop13:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(properties);
		producer.send(new ProducerRecord<String, String>("mydata", "six", "liu"));
		
		
		producer.close();*/
		
	KafkaUtil.deleteTopic("mydata");
		
	}

}
