import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class TweetsProducer {
	
	public TweetListener twListener;
	


	class TweetListener implements StatusListener {

		KafkaProducer producer;

		public TweetListener() {
			Properties props = new Properties();
			;
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producer = new KafkaProducer(props);
		}

		public void onStatus(Status status) {
			try {
				if (status.getLang().equals("en") && !status.isRetweet()) {
			    	String geoInfo = "37.7833,122.4167";
			    	String urlInfo = "n/a";
			    	String tweet = "n/a";
			    	if(status.getGeoLocation() != null)
			    	{
			    		geoInfo = String.valueOf(status.getGeoLocation().getLatitude()) + "," + String.valueOf(status.getGeoLocation().getLongitude());
			        	if(status.getURLEntities().length > 0)
			        	{
			        		for(URLEntity urlE: status.getURLEntities())
			        		{
			        			urlInfo = urlE.getURL();
			        		}         
			        	}
			       	   tweet = status.getText() + "DELIMITER" + geoInfo + "DELIMITER" + urlInfo;
					   System.out.println(tweet);
			       	   ProducerRecord<String, String> data = new ProducerRecord("tweets", tweet);
					   producer.send(data);
			    	}

				}
			} catch (Exception e) {
				System.out.println("wrong");
			}
		}

		public void onDeletionNotice(StatusDeletionNotice sdn) {
		}

		public void onTrackLimitationNotice(int i) {
		}

		public void onScrubGeo(long l, long l1) {
		}

		public void onStallWarning(StallWarning warning) {
		}

		public void onException(Exception e) {
			e.printStackTrace();
		}
	}

	public TweetsProducer(){
		this.twListener = new TweetListener();
	}
	
	public static void main(String[] args) {

		TwitterStream twitterStream;
		ConfigurationBuilder config = ConfigurationProvider.getConfig();

		TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

		twitterStream = fact.getInstance();

		TweetsProducer tp = new TweetsProducer();

		twitterStream.addListener(tp.twListener);

	    FilterQuery tweetFilterQuery = new FilterQuery(); // See 
	    tweetFilterQuery.locations(new double[][]{new double[]{-124.848974,24.396308},
	                    new double[]{-66.885444,49.384358
	                    }}); 
	    
	    tweetFilterQuery.language(new String[]{"en"});
	    twitterStream.filter(tweetFilterQuery);
		twitterStream.sample();
	}
}
