package uk.co.eelpieconsulting.monitoring.metricsrouter.destinations;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

@Component
public class MQTTPublisher implements MetricsDestination {

	private final String host;
	private final String topic;
	
	@Autowired
	public MQTTPublisher(@Value("${mqtt.host}") String host, @Value("${mqtt.topic}") String topic) {
		this.host = host;
		this.topic = topic;
		
		Map<String, String> m = Maps.newHashMap();
		m.put("test", "meh");
		publishMetrics(m);
	}
	
	@Override
	public void publishMetrics(Map<String, String> metrics) {
		try {
			MQTT mqtt = new MQTT();
			mqtt.setHost("tls://" + host + ":8883");			
			mqtt.setSslContext(sslContext());			
			
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
	
	private SSLContext sslContext() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, KeyManagementException {
		CertificateFactory cf = CertificateFactory.getInstance("X.509");
		X509Certificate caCert = (X509Certificate) cf.generateCertificate(getClass().getClassLoader().getResourceAsStream("eelpie.crt"));

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		ks.load(null); // You don't need the KeyStore instance to come from a file.
		ks.setCertificateEntry("caCert", caCert);

		tmf.init(ks);

		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, tmf.getTrustManagers(), null);
		return sslContext;
	}
	
}