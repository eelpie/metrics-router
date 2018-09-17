package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;

public class ZabbixRequestJsonBuilder {

  private final ObjectMapper mapper;

  public ZabbixRequestJsonBuilder() {
    mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
  }

  public String createAuthJson(String user, String password) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(null, "user.authenticate");

    final Map<String, String> params = Maps.newLinkedHashMap();
    params.put("user", user);
    params.put("password", password);
    call.put("params", params);

    call.put("id", 1);
    call.put("auth", null);

    String json = mapper.writeValueAsString(call);
    return json;
  }

  public String createAvailabilityJson(String authToken, Long from, Long to) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(authToken, "service.getsla");

    final Map<String, Object> params = Maps.newLinkedHashMap();
    params.put("servicesids", "1");

    Map<String, Long> intervals = Maps.newHashMap();
    intervals.put("from", from);
    intervals.put("to", to);
    params.put("intervals", intervals);

    call.put("params", params);
    call.put("id", 1);

    return mapper.writeValueAsString(call);
  }

  public String createGetItemsByKeyJson(String authToken, String key) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(authToken, "item.get");

    Map<String, Object> params = Maps.newLinkedHashMap();
    Map<String, String> filterParams = Maps.newLinkedHashMap();
    filterParams.put("key_", key);
    params.put("filter", filterParams);
    params.put("output", "extend");
    params.put("webitems", "true");

    call.put("params", params);
    call.put("id", 1);

    return mapper.writeValueAsString(call);
  }

  public String createHostsJson(String authToken) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(authToken, "host.get");

    final Map<String, String> params = Maps.newLinkedHashMap();
    params.put("output", "extend");
    call.put("params", params);
    call.put("id", 1);

    return mapper.writeValueAsString(call);
  }


  public String createGetTriggersJson(String authToken) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(authToken, "trigger.get");

    final Map<String, String> params = Maps.newLinkedHashMap();
    params.put("output", "extend");
    params.put("expandDescription", "true");

    params.put("monitored", "true");
    params.put("min_severity", "4");

    call.put("params", params);
    call.put("id", 1);

    return mapper.writeValueAsString(call);
  }

  public String createEventsRequestJson(String authToken, long triggerId) throws JsonProcessingException {
    final Map<String, Object> call = setupCall(authToken, "event.get");

    final Map<String, String> params = Maps.newLinkedHashMap();
    params.put("triggerids", Long.toString(triggerId));
    params.put("output", "extend");
    call.put("params", params);
    call.put("id", 1);

    return mapper.writeValueAsString(call);
  }

  private Map<String, Object> setupCall(String authToken, String method) {
    Map<String, Object> call = Maps.newLinkedHashMap();
    call.put("jsonrpc", "2.0");
    call.put("method", method);
    call.put("auth", authToken);
    return call;
  }

}