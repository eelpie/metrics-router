package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.n0tice;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import uk.co.eelpieconsulting.monitoring.metricsrouter.sources.MetricSource;

import com.google.common.collect.Maps;
import com.n0tice.api.client.N0ticeApi;
import com.n0tice.api.client.exceptions.N0ticeException;
import com.n0tice.api.client.model.AccessToken;
import com.n0tice.api.client.model.ResultSet;
import com.n0tice.api.client.model.SearchQuery;
import com.n0tice.api.client.model.User;

@Component
public class N0ticeStatisticsSource implements MetricSource {
	
	private static final String N0TICE_PREFIX = "n0tice-";
	
	private final N0ticeApi api;
	
	@Autowired
	public N0ticeStatisticsSource(@Value("${n0tice.apiUrl}") String apiUrl, 
			@Value("${n0tice.consumerKey}") String consumerKey,
			@Value("${n0tice.consumerSecret}") String consumerSecret,
			@Value("${n0tice.accessToken}") String accessToken,
			@Value("${n0tice.accessSecret}") String accessSecret) {
		this.api = new N0ticeApi(apiUrl, consumerKey, consumerSecret, new AccessToken(accessToken, accessSecret));
	}
	
	@Override
	public Map<String, String> getMetrics() {
		try {
			Map<String, String> metrics = Maps.newHashMap();	
			metrics.put(N0TICE_PREFIX + "approved", Integer.toString(api.search(new SearchQuery().limit(0)).getNumberFound()));		
			metrics.putAll(fetchUserSpecificMetrics());				
			return metrics;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public int getInterval() {
		return 60;
	}
	
	private Map<String, String> fetchUserSpecificMetrics() throws N0ticeException {
		final User user = api.verify();
		final String username = user.getUsername();
		
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
