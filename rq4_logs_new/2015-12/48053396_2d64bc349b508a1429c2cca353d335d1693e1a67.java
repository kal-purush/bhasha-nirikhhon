package com.graphaware.meetup;

import com.graphaware.meetup.engine.DummyEngine;
import com.graphaware.meetup.engine.MemberEngine;
import com.graphaware.meetup.engine.TopicEngine;
import com.graphaware.meetup.filter.GroupsBlackList;
import com.graphaware.meetup.processor.PenalizeBigGroups;
import com.graphaware.meetup.processor.RewardSameCity;
import com.graphaware.reco.generic.engine.RecommendationEngine;
import com.graphaware.reco.generic.filter.BlacklistBuilder;
import com.graphaware.reco.generic.filter.Filter;
import com.graphaware.reco.generic.log.Logger;
import com.graphaware.reco.generic.log.Slf4jRecommendationLogger;
import com.graphaware.reco.generic.log.Slf4jStatisticsLogger;
import com.graphaware.reco.generic.post.BasePostProcessor;
import com.graphaware.reco.generic.post.PostProcessor;
import com.graphaware.reco.neo4j.engine.Neo4jTopLevelDelegatingRecommendationEngine;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MyRecommendationEngine extends Neo4jTopLevelDelegatingRecommendationEngine {

    @Override
    protected List<RecommendationEngine<Node, Node>> engines() {
        return Arrays.<RecommendationEngine<Node, Node>>asList(
                new TopicEngine(),
                new MemberEngine()
        );
    }

    @Override
    protected List<PostProcessor<Node, Node>> postProcessors() {
        return Arrays.<PostProcessor<Node, Node>>asList(
                new RewardSameCity(),
                new PenalizeBigGroups()
        );
    }

    @Override
    protected List<BlacklistBuilder<Node, Node>> blacklistBuilders() {
        return Arrays.<BlacklistBuilder<Node, Node>>asList(
                new GroupsBlackList()
        );
    }

    @Override
    protected List<Filter<Node, Node>> filters() {
        return Collections.emptyList(); //todo add your own filters instead
    }

    @Override
    protected List<Logger<Node, Node>> loggers() {
        return Arrays.<Logger<Node, Node>>asList(
                new Slf4jRecommendationLogger<Node, Node>(),
                new Slf4jStatisticsLogger<Node, Node>()
        );
    }
}