package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import uk.co.eelpieconsulting.common.http.HttpBadRequestException;
import uk.co.eelpieconsulting.common.http.HttpFetchException;
import uk.co.eelpieconsulting.common.http.HttpFetcher;
import uk.co.eelpieconsulting.common.http.HttpForbiddenException;
import uk.co.eelpieconsulting.common.http.HttpNotFoundException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;

public class ZabbixApi {

  private final ZabbixRequestJsonBuilder zabbixRequestJsonBuilder;
  private final HttpFetcher httpFetcher;
  private final ObjectMapper objectMapper;
  private String zabbixUrl;

  public ZabbixApi(ZabbixRequestJsonBuilder zabbixRequestJsonBuilder, HttpFetcher httpFetcher, String zabbixUrl) {
    this.zabbixRequestJsonBuilder = zabbixRequestJsonBuilder;
    this.httpFetcher = httpFetcher;
    this.zabbixUrl = zabbixUrl;
    this.objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
  }

  public String getAuthToken(String user, String password) throws JsonParseException, JsonMappingException, HttpNotFoundException,
          HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {
    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createAuthJson(user, password));
    final Map<Object, Object> readValue = objectMapper.readValue(httpFetcher.post(httpPost), Map.class);
    return (String) readValue.get("result");
  }

  public Map<String, String> getHosts(final String authToken) throws UnsupportedEncodingException, JsonProcessingException,
          HttpNotFoundException, HttpBadRequestException,
          HttpForbiddenException, HttpFetchException, IOException,
          JsonParseException, JsonMappingException {
    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createHostsJson(authToken));
    Map readValue = objectMapper.readValue(httpFetcher.post(httpPost), Map.class);
    List<Map<String, Object>> results = (List<Map<String, Object>>) readValue.get("result");

    Map<String, String> hosts = Maps.newHashMap();
    for (Map<String, Object> map : results) {
      hosts.put((String) map.get("hostid"), (String) map.get("host"));
    }
    return hosts;
  }

  public Map getEvents(String authToken, long triggerId) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, IOException, HttpFetchException {
    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createEventsRequestJson(authToken, triggerId));
    return objectMapper.readValue(httpFetcher.post(httpPost), Map.class);
  }

  public Map getItemsByKey(final String authToken, String key) throws UnsupportedEncodingException, JsonProcessingException,
          HttpNotFoundException, HttpBadRequestException,
          HttpForbiddenException, HttpFetchException, IOException,
          JsonParseException, JsonMappingException {
    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createGetItemsByKeyJson(authToken, key));
    return objectMapper.readValue(httpFetcher.post(httpPost), Map.class);
  }

  @SuppressWarnings("unchecked")
  public Map<String, String> getTriggerStates(String authToken) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, HttpFetchException, IOException {
    Map<String, String> triggerMetrics = Maps.newHashMap();

    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createGetTriggersJson(authToken));
    final List<Map<String, Object>> results = (List<Map<String, Object>>) objectMapper.readValue(httpFetcher.post(httpPost), Map.class).get("result");
    int numberOfActiveTriggers = 0;
    for (Map<String, Object> result : results) {
      final boolean isTriggered = !result.get("value").equals("0");
      if (isTriggered) {
        numberOfActiveTriggers++;
      }

      final String label = (String) result.get("description");
      triggerMetrics.put(label, Boolean.toString(!isTriggered));
    }

    triggerMetrics.put("numberOfActiveTriggers", Integer.toString(numberOfActiveTriggers));
    triggerMetrics.put("activeTriggers", Boolean.toString(numberOfActiveTriggers > 0));
    return triggerMetrics;
  }

  @SuppressWarnings("unchecked")
  public Map<String, String> getTriggers(String authToken) throws HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, HttpFetchException, IOException {
    final Map<String, String> triggers = Maps.newHashMap();

    final HttpPost httpPost = setupApiPost(zabbixRequestJsonBuilder.createGetTriggersJson(authToken));
    String json = httpFetcher.post(httpPost);
    final List<Map<String, Object>> results = (List<Map<String, Object>>) objectMapper.readValue(json, Map.class).get("result");
    for (Map<String, Object> result : results) {
      final String triggerId = (String) result.get("triggerid");
      final String label = (String) result.get("description");
      triggers.put(triggerId, label);
    }

    return triggers;
  }

  private HttpPost setupApiPost(String json) throws UnsupportedEncodingException {
    final HttpPost httpPost = new HttpPost(zabbixUrl + "/api_jsonrpc.php");
    httpPost.addHeader("Content-Type", "application/json-rpc");
    httpPost.setEntity(new StringEntity(json));
    return httpPost;
  }

}
