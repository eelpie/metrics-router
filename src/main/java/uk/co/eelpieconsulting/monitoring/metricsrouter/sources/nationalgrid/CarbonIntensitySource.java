package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.nationalgrid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import uk.co.eelpieconsulting.common.http.HttpFetcher;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.zabbix.ZabbixAvailabilityMetricsSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class CarbonIntensitySource implements MetricSource {

    private static final Logger log = Logger.getLogger(ZabbixAvailabilityMetricsSource.class);

    private final String INTENSITY_ENDPOINT = "https://api.carbonintensity.org.uk/intensity";
    private final int SOUTH_ENGLAND_REGION = 12;
    private final String INTENSITY_REGIONAL_ENDPOINT = "https://api.carbonintensity.org.uk/regional/regionid/" + SOUTH_ENGLAND_REGION;

    private final ObjectMapper mapper;

    public CarbonIntensitySource() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.WRITE_NULL_MAP_VALUES);
        this.mapper = mapper;
    }

    @Override
    public Map<String, String> getMetrics() {
        try {
            log.info("Fetching current intensity: " + INTENSITY_ENDPOINT);
            Map<String, String> nationalResults = parseJson(new HttpFetcher().get(INTENSITY_ENDPOINT));
            log.info("Fetching current regional intensity: " + INTENSITY_REGIONAL_ENDPOINT);
            Map<String, String> regionalResults = parseJson(new HttpFetcher().get(INTENSITY_REGIONAL_ENDPOINT));

            Map<String, String> combined = Maps.newHashMap(nationalResults);
            combined.putAll(regionalResults);
            return combined;

        } catch (Exception e) {
            log.error(e);
            return Maps.newHashMap();
        }
    }

    protected HashMap<String, String> parseJson(String json) throws IOException {
        JsonNode jsonNode = mapper.readTree(json);
        JsonNode intensity = jsonNode.path("data").get(0).path("intensity");

        HashMap<String, String> result = Maps.newHashMap();
        for (String i: Lists.newArrayList("forecast", "actual")) {
            JsonNode value = intensity.get(i);
            if (value.isInt()) {
                result.put("carbonintensity." + i, Integer.toString(value.intValue()));
            }
        }
        return result;
    }

    @Override
    public int getInterval() {
        return 300;
    }

}


