package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.common.http.HttpBadRequestException;
import uk.co.eelpieconsulting.common.http.HttpFetchException;
import uk.co.eelpieconsulting.common.http.HttpFetcher;
import uk.co.eelpieconsulting.common.http.HttpForbiddenException;
import uk.co.eelpieconsulting.common.http.HttpNotFoundException;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Component
public class ZabbixAvailabilityMetricsSource implements MetricSource {

    private static final Logger log = Logger.getLogger(ZabbixAvailabilityMetricsSource.class);

    private static final String ZABBIX_GOOD_STATE = "0";
	private static final String ZABBIX_BAD_STATE = "1";
	private static final String ZABBIX_UNKNOWN_STATE = "2";

	private final ZabbixApi zabbixApi;
	private final String zabbixUsername;
	private final String zabbixPassword;
	
	private final List<String> triggerIds;
	
	@Autowired
	public ZabbixAvailabilityMetricsSource(@Value("${zabbix.url}") String zabbixUrl,
			@Value("${zabbix.username}") String zabbixUsername, 
			@Value("${zabbix.password}") String zabbixPassword,
			@Value("${zabbix.triggers}") String triggers) {
		this.zabbixUsername = zabbixUsername;
		this.zabbixPassword = zabbixPassword;
		
		this.zabbixApi = new ZabbixApi(new ZabbixRequestJsonBuilder(), new HttpFetcher(), zabbixUrl);		
		this.triggerIds = Lists.newArrayList(Splitter.on(",").split(triggers));
		log.info("Using triggers:" + triggerIds);		
	}
	
	@Override
	public int getInterval() {
		return 3600;
	}
	
	@Override
	public Map<String, String> getMetrics() {
		Map<String, String> metrics = Maps.newHashMap();

		DateTime end = new DateTime(DateTime.now().toDateMidnight());
		DateTime start = end.minusMonths(1);		
		for (String  triggerId : triggerIds) {
			try {
				metrics.putAll(populateDayMetrics(start, end, Integer.parseInt(triggerId)));
			} catch (Exception e) {
				log.error(e);
			}
		}
		
		end = new DateTime(DateTime.now().getYear(), DateTime.now().getMonthOfYear(), 1, 0, 0, 0, 0);
		start = end.minusMonths(6);		
		for (String  triggerId : triggerIds) {
			try {
				metrics.putAll(populateMonthMetrics(start, end, Integer.parseInt(triggerId)));
			} catch (Exception e) {
				log.error(e);
			}
		}
		
		return metrics;
	}

	private Map<String, String> populateDayMetrics(final DateTime start, final DateTime end, int triggerId) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {
		log.info("Extracting day availability for trigger " + triggerId + " from " + start + " to " + end);
				
		final String authToken = zabbixApi.getAuthToken(zabbixUsername, zabbixPassword);
		final Map<String, String> triggers = zabbixApi.getTriggers(authToken);
		final Map<String, Object> events = fetchEventLogForTrigger(triggerId, authToken);
		
		final Map<String, String> metrics = Maps.newHashMap();
		DateTime dayStart = new DateTime(start);
		while (dayStart.isBefore(end)) {
			DateTime dayEnd = dayStart.plusDays(1);
			metrics.put(makeMetricLabelForDay(Integer.toString(triggerId), dayStart, triggers), calculateAvailabilityFromEventLog(dayStart, dayEnd, events).toString());			
			dayStart = dayStart.plusDays(1);
		}
		return metrics;
	}
	
	private Map<String, String> populateMonthMetrics(final DateTime start, final DateTime end, int triggerId) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {
		log.info("Extracting month availability for trigger " + triggerId + " from " + start + " to " + end);
				
		final String authToken = zabbixApi.getAuthToken(zabbixUsername, zabbixPassword);
		final Map<String, String> triggers = zabbixApi.getTriggers(authToken);
		final Map<String, Object> events = fetchEventLogForTrigger(triggerId, authToken);
		
		final Map<String, String> metrics = Maps.newHashMap();
		DateTime monthStart = new DateTime(start);
		while (monthStart.isBefore(end)) {
			DateTime monthEnd = monthStart.plusMonths(1);			
			metrics.put(makeMetricLabelForMonth(Integer.toString(triggerId), monthStart, triggers), calculateAvailabilityFromEventLog(monthStart, monthEnd, events).toString());			
			monthStart = monthStart.plusMonths(1);
		}
		return metrics;
	}
	
	private String makeMetricLabelForDay(String triggerId, DateTime dayStart, Map<String, String> triggers) {		
		final String triggerLabel = triggers.containsKey(triggerId) ? triggers.get(triggerId) : triggerId;
		return triggerLabel + "-availability-" + ISODateTimeFormat.basicDate().print(dayStart);	// TODO trigger label
	}
	
	private String makeMetricLabelForMonth(String triggerId, DateTime monthStart,  Map<String, String> triggers) {
		final String triggerLabel = triggers.containsKey(triggerId) ? triggers.get(triggerId) : triggerId;
		return triggerLabel + "-availability-" +  DateTimeFormat.forPattern("YYYYMM").print(monthStart);
	}

	private Map<String, Object> fetchEventLogForTrigger(int triggerId, String authToken) throws JsonParseException, JsonMappingException, HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {
		log.info("Fetching event log for trigger: " + triggerId);
		return (Map<String, Object>) zabbixApi.getEvents(authToken, triggerId);
	}
	
	private BigDecimal calculateAvailabilityFromEventLog(DateTime from, DateTime to, Map<String, Object> events) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {		
		long unknown = 0;
		long good = 0;
		long bad = 0;
		
		DateTime start = from;
		String state = ZABBIX_UNKNOWN_STATE;
		final Iterator iterator = ((List) events.get("result")).iterator();				
		while(iterator.hasNext() && start.isBefore(to)) {
			
			Map<String, String> next = (Map<String, String>) iterator.next();
			long clock = Long.parseLong(next.get("clock"));			
			DateTime dateTime = new DateTime(clock * 1000);
			if (dateTime.isAfter(from)) {
				if (start.isBefore(from)) {
					start = from;
				}
				if (dateTime.isAfter(to)) {
					dateTime = to;
				}				
				long delta = dateTime.getMillis() - start.getMillis();
				if (state.equals(ZABBIX_GOOD_STATE)) {
					good = good + delta;
				}
				if (state.equals(ZABBIX_BAD_STATE)) {
					bad = bad + delta;
				}
				if (state.equals(ZABBIX_UNKNOWN_STATE)) {
					unknown = unknown + delta;
				}
			}
			
			state = next.get("value");				
			start = dateTime;
		}
					
		BigDecimal divide = new BigDecimal(bad).setScale(4).divide(new BigDecimal(good).setScale(4), 4);
		divide.setScale(4);		
		return new BigDecimal(1).subtract(divide).multiply(new BigDecimal(100)).setScale(2);
	}
	
}
