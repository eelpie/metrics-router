package uk.co.eelpieconsulting.monitoring.metricsrouter.destinations;
import java.util.Map;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;

@Component
public class MQTTPublisher implements MetricsDestination {

	private final String host;
	private final String topic;
	
	@Autowired
	public MQTTPublisher(@Value("${mqtt.host}") String host, @Value("${mqtt.topic}") String topic) {
		this.host = host;
		this.topic = topic;
	}
	
	@Override
	public void publishMetrics(Map<String, String> metrics) {
		try {
			MQTT mqtt = new MQTT();
			mqtt.setHost(host, 1883);
			BlockingConnection connection = mqtt.blockingConnection();
			connection.connect();		
				
			for (String metric : metrics.keySet()) {
				String value = metrics.get(metric);
				publish(connection, metric, value);			
			}
		
			connection.disconnect();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void publish(BlockingConnection connection, String metric, String value) throws Exception {
		String message = metric + (!Strings.isNullOrEmpty(value) ? ":" + value : "");
		connection.publish(topic, message.getBytes(), QoS.AT_MOST_ONCE, false);
	}
	
}
