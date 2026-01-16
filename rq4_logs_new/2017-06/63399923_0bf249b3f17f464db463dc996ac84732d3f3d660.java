package com.pluralsight.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class FilterEnglishTweets {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "...");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
        props.setProperty(TwitterSource.TOKEN, "...");
        props.setProperty(TwitterSource.TOKEN_SECRET, "...");


        env.addSource(new TwitterSource(props))
                .map(new MapToTweets())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return tweet.getLanguage().equals("en");
                    }
                })
                .print();

        env.execute();
    }
}
