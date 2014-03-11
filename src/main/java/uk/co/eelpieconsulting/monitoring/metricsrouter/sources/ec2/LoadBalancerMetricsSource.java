package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.ec2;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Component
public class LoadBalancerMetricsSource implements MetricSource {
	
	private static final int ONE_MINUTE = 60;
	private static final int ONE_HOUR = ONE_MINUTE * 60;
	private static final int ONE_DAY = ONE_HOUR * 24;
	private static final String DAY = "day";
	private static final String HOUR = "hour";
	private static final String MINUTE = "minute";
	private static final String HTTP_CODE_BACKEND_5XX = "HTTPCode_Backend_5XX";
	private static final String REQUEST_COUNT = "RequestCount";
	private static final String LOAD_BALANCER_NAME = "LoadBalancerName";
	private static final String AWS_ELB_NAMESPACE = "AWS/ELB";
	
	private static final NumberFormat integer = new DecimalFormat("#");
	private static final NumberFormat threeDecimalPlacesFormat = new DecimalFormat("#.###");

	private String accessKey;
	private String accessSecret;
	private String regionName;
	private List<String> loadBalancers;
	
	@Autowired
	public LoadBalancerMetricsSource(@Value("${ec2.accessKey}")String accessKey, 
			@Value("${ec2.accessSecret}") String accessSecret,
			@Value("${ec2.regionName}") String regionName, 
			@Value("${ec2.loadBalancers}") String loadBalancers) {
		this.accessKey = accessKey;
		this.accessSecret = accessSecret;
		this.regionName = regionName;
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
		final AmazonCloudWatchClient amazonCloudWatchClient = getCloudWatchClient(accessKey, accessSecret, regionName);
		
		final Map<String, String> metrics = Maps.newLinkedHashMap();
		
		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastMinuteOf(loadBalancerRequestCount(loadBalancer, REQUEST_COUNT))), MINUTE);
		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastHourOf(loadBalancerRequestCount(loadBalancer, REQUEST_COUNT))), HOUR);
		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastDayOf(loadBalancerRequestCount(loadBalancer, REQUEST_COUNT))), DAY);

		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastMinuteOf(loadBalancerRequestCount(loadBalancer, HTTP_CODE_BACKEND_5XX))), MINUTE);
		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastHourOf(loadBalancerRequestCount(loadBalancer, HTTP_CODE_BACKEND_5XX))), HOUR);	
		parseDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastDayOf(loadBalancerRequestCount(loadBalancer, HTTP_CODE_BACKEND_5XX))), DAY);
				
		parseAverageDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastMinuteOf(loadBalancerLatency(loadBalancer))), MINUTE);
		parseAverageDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastHourOf(loadBalancerLatency(loadBalancer))), HOUR);
		parseAverageDataPoints(loadBalancer, metrics, amazonCloudWatchClient.getMetricStatistics(lastDayOf(loadBalancerLatency(loadBalancer))), DAY);
		
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

	private GetMetricStatisticsRequest lastMinuteOf(GetMetricStatisticsRequest request) {
		request.withStartTime(new DateTime().minusMinutes(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_MINUTE);
		return request;
	}
	
	private GetMetricStatisticsRequest lastHourOf(GetMetricStatisticsRequest request) {
		request.withStartTime(new DateTime().minusHours(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_HOUR);
		return request;
	}
	
	private GetMetricStatisticsRequest lastDayOf(GetMetricStatisticsRequest request) {
		request.withStartTime(new DateTime().minusDays(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_DAY);
		return request;
	}

	private void parseDataPoints(String loadBalancer, final Map<String, String> metrics, GetMetricStatisticsResult metricResult, String suffix) {
		final Double sum = !metricResult.getDatapoints().isEmpty() ? metricResult.getDatapoints().get(0).getSum() : 0;
		metrics.put(loadBalancer + "-" + metricResult.getLabel() + "-" + suffix, integer.format(sum));
	}
	
	private void parseAverageDataPoints(String loadBalancer, final Map<String, String> metrics, GetMetricStatisticsResult metricResult, String suffix) {
		final Double average = !metricResult.getDatapoints().isEmpty() ? metricResult.getDatapoints().get(0).getAverage() : 0;
		metrics.put(loadBalancer + "-" + metricResult.getLabel() + "-" + suffix, threeDecimalPlacesFormat.format(average));
	}

	private AmazonCloudWatchClient getCloudWatchClient(String accessKey, String accessSecret, String regionNames) {
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, accessSecret);
		AmazonCloudWatchClient amazonCloudWatchClient = new AmazonCloudWatchClient(credentials);
		Region region = RegionUtils.getRegion(regionName);
		amazonCloudWatchClient.setRegion(region);
		return amazonCloudWatchClient;
	}
	
	private GetMetricStatisticsRequest loadBalancerRequestCount(String loadBalancer, String metricName) {
		GetMetricStatisticsRequest getMetricStatisticsRequest = new GetMetricStatisticsRequest()	
			.withUnit(StandardUnit.Count)
			.withStatistics("Sum")
			.withNamespace(AWS_ELB_NAMESPACE)
			.withMetricName(metricName)
			.withDimensions(new Dimension().withName(LOAD_BALANCER_NAME).withValue(loadBalancer));
		return getMetricStatisticsRequest;
	}
	
	private GetMetricStatisticsRequest loadBalancerLatency(String loadBalancer) {
		GetMetricStatisticsRequest getMetricStatisticsRequest = new GetMetricStatisticsRequest()
			.withUnit(StandardUnit.Seconds)
			.withStatistics("Average")
			.withNamespace(AWS_ELB_NAMESPACE)
			.withMetricName("Latency")
			.withDimensions(new Dimension().withName(LOAD_BALANCER_NAME).withValue(loadBalancer));
		return getMetricStatisticsRequest;
	}
	
}
