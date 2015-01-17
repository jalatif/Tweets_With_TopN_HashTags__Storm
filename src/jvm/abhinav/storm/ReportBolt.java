package abhinav.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import abhinav.storm.tools.Rankings;
import abhinav.storm.tools.Rankable;

import java.util.Map;
import java.util.List;
import java.util.Arrays;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;
  private String[] skipWords = {"http://", "https://", "(", "a", "an", "the", "for", "retweet", "RETWEET", "follow", "FOLLOW"};
  private Rankings last_seen_final_rankings;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
    last_seen_final_rankings = null;
  }

  @Override
  public void execute(Tuple tuple)
  {
    String boltId = tuple.getSourceComponent(); // get the source of the tweet
    if (boltId.equals("tweet-spout") && last_seen_final_rankings != null) {
      String tweet = tuple.getString(0); // get the tweet
      // provide the delimiters for splitting the tweet
      String delims = "[ .,?!]+";
      // now split the tweet into tokens
      String[] tokens = tweet.split(delims);
      // for each token/word, emit it
      for (String word : tokens) {
        // check if number of words in a tweet is > 3 otherwise don't emit
        if (word.length() > 3 && !Arrays.asList(skipWords).contains(word)) {

          if (word.startsWith("#")) { // a hash tag
            List<Rankable> ranks = last_seen_final_rankings.getRankings();

            for (Rankable r : ranks) {
              // access the first column 'word'
              String hash_word = (String) r.getObject();//tuple.getStringByField("word");

              // access the second column 'count'
              Long count = r.getCount();
              //Integer count = tuple.getIntegerByField("count");

              //redis.publish("WordCountTopology", word + "*****|" + Long.toString(30L));
              if (word.equals(hash_word)){
                redis.publish("WordCountTopology", tweet + "|" + Long.toString(count));
              }
            }
          }
        }
      }
    } else if (boltId.equals("total-rankings-bolt")) {
      // Save the last seen final top N hash tags so that can be compared with tweets hashes.
      last_seen_final_rankings = (Rankings) tuple.getValue(0);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
