package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.ec2;

import org.joda.time.DateTime;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;

public class RequestBuilder {

	private static final String AWS_ELB_NAMESPACE = "AWS/ELB";
	private static final String AWS_RDS_NAMESPACE = "AWS/RDS";
	private static final String DB_INSTANCE_IDENTIFIER = "DBInstanceIdentifier";
	private static final String LOAD_BALANCER_NAME = "LoadBalancerName";		
	private static final int ONE_MINUTE = 60;
	private static final int ONE_HOUR = 60 * 60;
	private static final int ONE_DAY = ONE_HOUR * 24;
	
	private GetMetricStatisticsRequest request;
	
	public RequestBuilder loadBalancerRequestCount(String loadBalancer, String metricName) {
		this.request = new GetMetricStatisticsRequest()	
			.withUnit(StandardUnit.Count)
			.withStatistics("Sum")
			.withNamespace(AWS_ELB_NAMESPACE)
			.withMetricName(metricName)
			.withDimensions(new Dimension().withName(LOAD_BALANCER_NAME).withValue(loadBalancer));
		return this;
	}
	
	public RequestBuilder loadBalancerLatency(String loadBalancer) {
		this.request = new GetMetricStatisticsRequest()
			.withUnit(StandardUnit.Seconds)
			.withStatistics("Average")
			.withNamespace(AWS_ELB_NAMESPACE)
			.withMetricName("Latency")
			.withDimensions(new Dimension().withName(LOAD_BALANCER_NAME).withValue(loadBalancer));
		return this;
	}
	
	public RequestBuilder freeStorageSpaceFor(String database) {
		this.request = new GetMetricStatisticsRequest()
			.withStatistics("Average")		
			.withNamespace(AWS_RDS_NAMESPACE)
			.withMetricName("FreeStorageSpace")
			.withDimensions(new Dimension().withName(DB_INSTANCE_IDENTIFIER).withValue(database));
		return this;
	}
	
	public GetMetricStatisticsRequest lastMinuteOf() {
		request.withStartTime(new DateTime().minusMinutes(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_MINUTE);
		return request;
	}
	
	public GetMetricStatisticsRequest lastHourOf() {
		request.withStartTime(new DateTime().minusHours(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_HOUR);
		return request;
	}
	
	public GetMetricStatisticsRequest lastDayOf() {
		request.withStartTime(new DateTime().minusDays(1).toDate()).
			withEndTime(new DateTime().minusHours(0).toDate()).
			withPeriod(ONE_DAY);
		return request;
	}
	
}
