package uk.co.eelpieconsulting.monitoring.metricsrouter.sources;

import java.util.Map;

public interface MetricSource {

  public Map<String, String> getMetrics();

  public int getInterval();

}