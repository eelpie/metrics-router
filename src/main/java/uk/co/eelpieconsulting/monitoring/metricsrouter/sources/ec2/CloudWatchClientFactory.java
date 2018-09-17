package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.ec2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;

@Component
public class CloudWatchClientFactory {

  private final String accessKey;
  private final String accessSecret;
  private final String regionName;

  @Autowired
  public CloudWatchClientFactory(@Value("${ec2.accessKey}") String accessKey,
                                 @Value("${ec2.accessSecret}") String accessSecret,
                                 @Value("${ec2.regionName}") String regionName) {
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.regionName = regionName;
  }

  public AmazonCloudWatchClient getCloudWatchClient() {
    AWSCredentials credentials = new BasicAWSCredentials(accessKey, accessSecret);
    AmazonCloudWatchClient amazonCloudWatchClient = new AmazonCloudWatchClient(credentials);
    Region region = RegionUtils.getRegion(regionName);
    amazonCloudWatchClient.setRegion(region);
    return amazonCloudWatchClient;
  }

}
