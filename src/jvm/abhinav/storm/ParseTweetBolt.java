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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt
{
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;
  //String skipWords[] = {"http", "abc", "you", "#UFC182"};
  private String[] skipWords = {"http://", "https://", "(", "a", "an", "the", "for", "retweet", "RETWEET", "follow", "FOLLOW"};

  Set<String> skipWordsList;
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    skipWordsList = new TreeSet<String>();
    for (String word : skipWords)
      skipWordsList.add(word);
  }

  @Override
  public void execute(Tuple tuple)
  {
    // get the 1st column 'tweet' from tuple
    String tweet = tuple.getString(0);

    // provide the delimiters for splitting the tweet
    String delims = "[ .,?!]+";

    // now split the tweet into tokens
    String[] tokens = tweet.split(delims);

    // for each token/word, emit it
    for (String token: tokens) {
      if (token.length() < 3) continue;
      if (inSkipList(token)) continue;
      if (!isHashTag(token)) continue;
      collector.emit(new Values(token));
    }
  }

  private boolean inSkipList(String word) {
    if (skipWordsList.contains(word))
      return true;
    return false;
  }

  private boolean isHashTag(String word) {
    if (word.startsWith("#"))
      return true;
    return false;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare(new Fields("tweet-word"));
  }
}
