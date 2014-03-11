package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.elasticsearch;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.common.http.HttpBadRequestException;
import uk.co.eelpieconsulting.common.http.HttpFetchException;
import uk.co.eelpieconsulting.common.http.HttpFetcher;
import uk.co.eelpieconsulting.common.http.HttpForbiddenException;
import uk.co.eelpieconsulting.common.http.HttpNotFoundException;
import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

@Component
public class ElasticSearchClusterStatsSource implements MetricSource {

	private final String elasticSearchUrl;
	
	@Autowired
	public ElasticSearchClusterStatsSource(@Value("${elasticsearch.url}") String elasticSearchUrl) {
		this.elasticSearchUrl = elasticSearchUrl;
	}
	
	@Override
	public Map<String, String> getMetrics() {
		try {
			final InputStream clusterStatsJson = fetchClusterStats();	
			return extractHeapUsageStats(clusterStatsJson);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
	
	@Override
	public int getInterval() {
		return 60;
	}
	
	private InputStream fetchClusterStats() throws UnsupportedEncodingException, HttpNotFoundException, HttpBadRequestException, HttpForbiddenException, HttpFetchException {
		return new StringInputStream(new HttpFetcher().get(elasticSearchUrl + "/_cluster/nodes/stats?jvm=true"));
	}
	
	private Map<String, String> extractHeapUsageStats(InputStream clusterStatsJson) throws IOException, JsonProcessingException {
		JsonNode jsonTree = new ObjectMapper().readTree(clusterStatsJson);				
		JsonNode nodes = jsonTree.findPath("nodes");
		
		Map<String, String> heapUsage = Maps.newHashMap();
		for (JsonNode node : nodes) {
			final boolean isClient = node.findPath("client").asBoolean();
			if (isClient) {
				continue;
			}
			
			JsonNode jvm = node.findPath("jvm");
			String heapUsedPercent = jvm.findValue("heap_used_percent").asText();
			final String hostname = node.get("hostname").asText();
			heapUsage.put("elasticsearch-" + hostname + "-heapUsedPercent", heapUsedPercent);			
		}
		
		IOUtils.closeQuietly(clusterStatsJson);
		return heapUsage;
	}
	
}
