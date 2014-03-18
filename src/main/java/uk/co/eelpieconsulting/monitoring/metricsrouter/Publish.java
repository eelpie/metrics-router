package uk.co.eelpieconsulting.monitoring.metricsrouter;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.monitoring.metricsrouter.destinations.MetricsDestination;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.google.common.collect.Maps;

@Component
public class Publish {
	
    private static final Logger log = Logger.getLogger(Publish.class);
	
	private List<MetricSource> metricSources;
	private List<MetricsDestination> metricDestinations;
	private Map<MetricSource, Date> lastRun;
	
	@Autowired
	public Publish(List<MetricSource> metricSources, List<MetricsDestination> metricDestinations) {
		
		this.metricSources = metricSources;
		this.metricDestinations = metricDestinations;
		
		lastRun = Maps.newHashMap();
		
		log.info("Initialised with metric sources: " + metricSources);
		log.info("Initialised with metric destinations: " + metricDestinations);
	}
		
	@Scheduled(fixedDelay=5000)
	public void publish() throws Exception {
		log.debug("Polling");
		for (MetricSource metricSource : metricSources) {
			try {
				if (shouldRun(metricSource)) {
					log.debug("Polling metric source: " + metricSource.getClass().getSimpleName());
					final Map<String, String> metricsFromSource = metricSource.getMetrics();
					recordMetricSourceLastRunTime(metricSource);					
					publishMetrics(metricsFromSource);					
				}
				
			} catch (Exception e) {
				log.error("Unexpected exception while polling metrics source: " + metricSource.getClass().getSimpleName(), e);
			}
		}	
	}
	
	private void publishMetrics(final Map<String, String> metrics) {
		log.debug("Publishing metrics: " + metrics);
		for (MetricsDestination metricsDestination : metricDestinations) {
			try {
				metricsDestination.publishMetrics(metrics);
				
			} catch (Exception e) {
				log.error("Unexpected exception while publishing metrics to destination: " + metricsDestination.getClass().getSimpleName(), e);
			}
		}
	}

	private boolean shouldRun(MetricSource metricSource) {
		final Date lastTimeThisMetricSourceRan = lastRun.get(metricSource);
		if (lastTimeThisMetricSourceRan == null) {
			return true;			
		}
		
		final Integer metricSourceRepeatInterval = metricSource.getInterval();
		return lastTimeThisMetricSourceRan.before(DateTime.now().minusSeconds(metricSourceRepeatInterval).toDate());
	}
	
	private void recordMetricSourceLastRunTime(MetricSource metricSource) {
		lastRun.put(metricSource, DateTime.now().toDate());
	}
	
}
