package uk.co.eelpieconsulting.monitoring.metricsrouter.destinations;

import java.util.Map;

public interface MetricsDestination {

	public void publishMetrics(Map<String, String> metrics);

}