package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@Component
public class ZabbixMetricsSource implements MetricSource {

  private static final Logger log = Logger.getLogger(ZabbixMetricsSource.class);

  private final List<String> metricKeys;

  private final String user;
  private final String password;
  private final ZabbixApi zabbixApi;

  @Autowired
  public ZabbixMetricsSource(@Value("${zabbix.url}") String zabbixUrl,
                             @Value("${zabbix.username}") String user,
                             @Value("${zabbix.password}") String password,
                             @Value("${zabbix.metrics}") String metrics) {
    this.user = user;
    this.password = password;
    this.metricKeys = Lists.newArrayList(Splitter.on("|").split(metrics));
    log.info("Using metrics:" + metricKeys);
    this.zabbixApi = new ZabbixApi(new ZabbixRequestJsonBuilder(), new HttpFetcher(), zabbixUrl);
  }

  @Override
  public int getInterval() {
    return 10;
  }

  public Map<String, String> getMetrics() {
    String authToken;
    try {
      authToken = zabbixApi.getAuthToken(user, password);
      Map<String, String> hosts = zabbixApi.getHosts(authToken);
      return getZabbixMetrics(authToken, hosts, metricKeys);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, String> getAvailability() {
    return Maps.newHashMap();
  }

  private Map<String, String> getZabbixMetrics(final String authToken, Map<String, String> hosts, List<String> keys)
          throws UnsupportedEncodingException, JsonProcessingException,
          HttpNotFoundException, HttpBadRequestException,
          HttpForbiddenException, HttpFetchException, IOException, JsonParseException, JsonMappingException {

    final Map<String, String> metrics = Maps.newHashMap();
    for (String key : keys) {
      fetchMetricByKey(authToken, hosts, metrics, key);
    }

    metrics.putAll(zabbixApi.getTriggerStates(authToken));
    return metrics;
  }

  private void fetchMetricByKey(final String authToken, Map<String, String> hosts, Map<String, String> metrics, String key) throws UnsupportedEncodingException, JsonProcessingException,
          HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, HttpFetchException, IOException, JsonParseException, JsonMappingException {
    Map readValue = zabbixApi.getItemsByKey(authToken, key);
    List<Map<String, Object>> results = (List) readValue.get("result");
    for (Map<String, Object> result : results) {
      final String hostid = (String) result.get("hostid");
      if (!hosts.containsKey(hostid)) {
        continue;
      }
      if (result.get("lastclock") == null) {
        continue;
      }

      final DateTime lastClock = new DateTime(Long.parseLong((String) result.get("lastclock")) * 1000);
      if (lastClock.isBefore(DateTime.now().minusMinutes(10))) {
        continue;
      }

      metrics.put(hosts.get(hostid) + "-" + key, (String) result.get("lastvalue"));    // TODO can get API to return the resolved hostname?
    }
  }

}
