package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.n0tice;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.n0tice.api.client.N0ticeApi;
import com.n0tice.api.client.exceptions.N0ticeException;
import com.n0tice.api.client.model.AccessToken;
import com.n0tice.api.client.model.ResultSet;
import com.n0tice.api.client.model.SearchQuery;

@Component
public class N0ticeStatisticsSource implements MetricSource {
	
    private static final Logger log = Logger.getLogger(N0ticeStatisticsSource.class);
    
	private static final String N0TICE_PREFIX = "n0tice-";
	
	private final String apiUrl;
	private final String consumerKey;
	private final String consumerSecret;
	private final List<AccessToken> accessTokens;
	
	@Autowired
	public N0ticeStatisticsSource(@Value("${n0tice.apiUrl}") String apiUrl, 
			@Value("${n0tice.consumerKey}") String consumerKey,
			@Value("${n0tice.consumerSecret}") String consumerSecret,
			@Value("${n0tice.accessTokens}") String accessTokenStrings) {		
		this.apiUrl = apiUrl;
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
			
		this.accessTokens = Lists.newArrayList();
		Iterable<String> tokenPairs = Splitter.on(",").split(accessTokenStrings);
		for (String tokenPair : tokenPairs) {
			final String[] parts = tokenPair.split("\\|");
			AccessToken accessToken = new AccessToken(parts[0], parts[1]);
			log.debug("Adding access token: " + accessToken);
			accessTokens.add(accessToken);
		}
	}
	
	@Override
	public Map<String, String> getMetrics() {
		final Map<String, String> metrics = Maps.newHashMap();
		for (AccessToken accessToken : accessTokens) {
			try {
				metrics.putAll(fetchUserSpecificMetrics(accessToken));					
			} catch (Exception e) {
				log.error(e);
			}
		}
		return metrics;
	}
	
	@Override
	public int getInterval() {
		return 60;
	}
	
	private Map<String, String> fetchUserSpecificMetrics(AccessToken accessToken) throws N0ticeException {
		final N0ticeApi api = new N0ticeApi(apiUrl, consumerKey, consumerSecret, accessToken);
		final String username = api.verify().getUsername();
		log.info("Polling for metrics for user: " + username);
		
		Map<String, String> userSpecificMetrics = Maps.newHashMap();		
		ResultSet approved = api.search(new SearchQuery().noticeBoardOwnedBy(username).limit(0));		
		userSpecificMetrics.put(N0TICE_PREFIX + username + "-approved", Integer.toString(approved.getNumberFound()));		
		
		ResultSet all = api.authedSearch(new SearchQuery().noticeBoardOwnedBy(username).limit(0));		
		userSpecificMetrics.put(N0TICE_PREFIX + username + "-all", Integer.toString(all.getNumberFound()));		
		
		ResultSet awaiting = api.authedSearch(new SearchQuery().noticeBoardOwnedBy(username).internalModerationStatus("AWAITING").limit(0));		
		userSpecificMetrics.put(N0TICE_PREFIX + username + "-awaiting", Integer.toString(awaiting.getNumberFound()));
		
		ResultSet approvedVideosWithNoYouTubeId = api.authedSearch(new SearchQuery().noticeBoardOwnedBy(username).moderationStatus("APPROVED").hasVideos(true).hasYouTubeId(false));
		userSpecificMetrics.put(N0TICE_PREFIX + username + "-noyoutubeid", Integer.toString(approvedVideosWithNoYouTubeId.getNumberFound()));
				
		return userSpecificMetrics;
	}

}
