package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.nationalgrid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import uk.co.eelpieconsulting.common.http.HttpFetcher;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix.ZabbixAvailabilityMetricsSource;

import java.util.HashMap;
import java.util.Map;

public class CarbonIntensitySource implements MetricSource {

    private static final Logger log = Logger.getLogger(ZabbixAvailabilityMetricsSource.class);

    private final String INTENSITY_ENDPOINT = "https://api.carbonintensity.org.uk/intensity";

    @Override
    public Map<String, String> getMetrics() {
        try {
            log.info("Fetching current intensity: " + INTENSITY_ENDPOINT);
            String json = new HttpFetcher().get(INTENSITY_ENDPOINT);
            log.info("Got JSON: " + json);

            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);

            JsonNode jsonNode = mapper.readTree(json);

            JsonNode path = jsonNode.findPath("data/intensity/forecast");
            int forecast = path.asInt();
            HashMap<String, String> result = Maps.newHashMap();
            result.put("carbonintensity.forecast", Integer.toString(forecast));
            return result;

        } catch (Exception e) {
            log.error(e);
            return Maps.newHashMap();
        }
    }

    @Override
    public int getInterval() {
        return 60;
    }

}


