package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.ec2;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Component
public class RDSMetricsSource implements MetricSource {
	
	private static final String MINUTE = "minute";
	private static final NumberFormat integer = new DecimalFormat("#");
	
	private final CloudWatchClientFactory cloudWatchClientFactory;
	private final List<String> databases;
	
	@Autowired
	public RDSMetricsSource(CloudWatchClientFactory cloudWatchClientFactory, @Value("${ec2.databases}") String databases) {
		this.cloudWatchClientFactory = cloudWatchClientFactory;
		this.databases = Lists.newArrayList(Splitter.on(",").split(databases));
	}
	
	@Override
	public Map<String, String> getMetrics() {
		final Map<String, String> metrics = Maps.newHashMap();
		for (String database : databases) {
			metrics.putAll(getRDSMetrics(database));
		}
		return metrics;
	}
	
	@Override
	public int getInterval() {
		return 300;
	}
	
	private Map<String, String> getRDSMetrics(String database) {
		final AmazonCloudWatchClient amazonCloudWatchClient = cloudWatchClientFactory.getCloudWatchClient();		

		final Map<String, String> metrics = Maps.newLinkedHashMap();
		parseAverageDataPoints(database, metrics, amazonCloudWatchClient.getMetricStatistics(new RequestBuilder().freeStorageSpaceFor(database).lastMinuteOf()), MINUTE);
		return metrics;
	}
	
	private void parseAverageDataPoints(String loadBalancer, final Map<String, String> metrics, GetMetricStatisticsResult metricResult, String suffix) {
		final Double average = !metricResult.getDatapoints().isEmpty() ? metricResult.getDatapoints().get(0).getAverage() : 0;
		metrics.put(loadBalancer + "-" + metricResult.getLabel() + "-" + suffix, integer.format(average));
	}
		
}
