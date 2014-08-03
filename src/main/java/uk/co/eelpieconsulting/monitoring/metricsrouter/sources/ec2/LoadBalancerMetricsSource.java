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
public class LoadBalancerMetricsSource implements MetricSource {
	
	private static final String DAY = "day";
	private static final String HOUR = "hour";
	private static final String MINUTE = "minute";
	private static final String HTTP_CODE_BACKEND_5XX = "HTTPCode_Backend_5XX";
	private static final String REQUEST_COUNT = "RequestCount";
	
	private static final NumberFormat integer = new DecimalFormat("#");
	private static final NumberFormat threeDecimalPlacesFormat = new DecimalFormat("#.###");

	private final CloudWatchClientFactory cloudWatchClientFactory;
	private final List<String> loadBalancers;
	
	@Autowired
	public LoadBalancerMetricsSource(CloudWatchClientFactory cloudWatchClientFactory, @Value("${ec2.loadBalancers}") String loadBalancers) {
		this.cloudWatchClientFactory = cloudWatchClientFactory;
		this.loadBalancers = Lists.newArrayList(Splitter.on(",").split(loadBalancers));
	}
	
	@Override
	public Map<String, String> getMetrics() {
		Map<String, String> metrics = Maps.newHashMap();		
		for (String loadBalancer : loadBalancers) {
			metrics.putAll(getLoadBalancerMetrics(loadBalancer));			
		}
		return metrics;
	}
	
	@Override
	public int getInterval() {
		return 60;
	}

	private Map<String, String> getLoadBalancerMetrics(String loadBalancer) {
		final AmazonCloudWatchClient amazonCloudWatchClient = cloudWatchClientFactory.getCloudWatchClient();
		
		final Map<String, String> metrics = Maps.newHashMap();
		
		RequestBuilder loadBalancerRequestCount = new RequestBuilder().loadBalancerRequestCount(loadBalancer, REQUEST_COUNT);
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerRequestCount.lastMinuteOf()), loadBalancer, MINUTE));
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerRequestCount.lastHourOf()), loadBalancer, HOUR));
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerRequestCount.lastDayOf()), loadBalancer, DAY));

		RequestBuilder loadBalancerFailedRequestCount = new RequestBuilder().loadBalancerRequestCount(loadBalancer, HTTP_CODE_BACKEND_5XX);
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerFailedRequestCount.lastMinuteOf()), loadBalancer, MINUTE));
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerFailedRequestCount.lastHourOf()), loadBalancer, HOUR));
		metrics.putAll(parseDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerFailedRequestCount.lastDayOf()), loadBalancer, DAY));
		
		RequestBuilder loadBalancerLatency = new RequestBuilder().loadBalancerLatency(loadBalancer);
		metrics.putAll(parseAverageDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerLatency.lastMinuteOf()), loadBalancer, MINUTE));
		metrics.putAll(parseAverageDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerLatency.lastHourOf()), loadBalancer, HOUR));
		metrics.putAll(parseAverageDataPoints(amazonCloudWatchClient.getMetricStatistics(loadBalancerLatency.lastDayOf()), loadBalancer, DAY));
		
		generateBadRequestPercentages(loadBalancer, metrics);
		return metrics;
	}

	private void generateBadRequestPercentages(String loadBalancer, final Map<String, String> metrics) {
		List<String> periodsToCalculatePercentagesFor = Lists.newArrayList(MINUTE, HOUR, DAY);
		for (String period : periodsToCalculatePercentagesFor) {
			long bad = Long.parseLong(metrics.get(loadBalancer + "-HTTPCode_Backend_5XX-" + period));
			long good = Long.parseLong(metrics.get(loadBalancer + "-RequestCount-" + period));
			
			final double percentage = calculatePercentage(bad, good);
			final String key = loadBalancer + "-HTTPCode_Backend_5XX-percentage-" + period;
			metrics.put(key, threeDecimalPlacesFormat.format(percentage));
		}
	}

	private double calculatePercentage(long bad, long good) {
		if (bad == 0) {
			return 0;
		}
		if (good == 0) {
			return 100;
		}
		return (bad * 1.0 / good * 1.0) * 100;
	}
	
	private Map<String, String> parseDataPoints(GetMetricStatisticsResult metricResult, String prefix, String suffix) {
		final Map<String, String> metrics = Maps.newHashMap();		
		final Double sum = !metricResult.getDatapoints().isEmpty() ? metricResult.getDatapoints().get(0).getSum() : 0;
		metrics.put(prefix + "-" + metricResult.getLabel() + "-" + suffix, integer.format(sum));
		return metrics;
	}
	
	private Map<String, String> parseAverageDataPoints(GetMetricStatisticsResult metricResult, String prefix, String suffix) {
		final Map<String, String> metrics = Maps.newHashMap();
		final Double average = !metricResult.getDatapoints().isEmpty() ? metricResult.getDatapoints().get(0).getAverage() : 0;
		metrics.put(prefix + "-" + metricResult.getLabel() + "-" + suffix, threeDecimalPlacesFormat.format(average));
		return metrics;
	}
	
}
