Realtime visualization of Tweets which contain any of the Top N #trending hashtags

1. Twitter4j apis to fetch live twitter streams. This requires creating a consumer and token keys and secret. Inetgrate with TwitterStreamFactory object and registration of a handler which receives the stream and dumps them to a queue. This queue is polled periodically and the tweet spout then emits available tweets forward to the processing bolts.

2. Storm topology consists of tweet-spoout----parse-tweet-bolt----count-bolt----intermediate-ranker----total-ranker----report-bolt | |

3. Tweet spout connect to twitter4j stream factory to fetch live streams of tweets. parse tweet bolt, filters hashtags and forwards to count bolt(fields grouping). count bolt aggregates the count and forwards to intermediate-ranker via fieldsGrouping. intermediate-ranker and total-ranker are off the shelf opensource bolts which spit out Top N word counts. This is passed to report-bolt which is connected by global grouping to both total-ranker bolt and tweet-spout directly. report bolt performs a join operation of the streams that it receives from tweet spout and ranker bolt, and emits only those tweets which contain atleast one hashtag that is in TopN. report-bolt passes the data onto to a d3.js based visualizer to display the tweets.

[This basic implementation of a storm toplogy is done as a part of Udacity's "Real Time analytics with apache storm" course. https://www.udacity.com/course/ud381]

