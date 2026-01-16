/*
 * This file is part of the Meeds project (https://meeds.io/).
 *
 * Copyright (C) 2025 Meeds Association contact@meeds.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */
package io.meeds.news.listener;

import io.meeds.analytics.model.StatisticData;
import io.meeds.analytics.utils.AnalyticsUtils;
import io.meeds.news.model.ArticleTarget;
import io.meeds.news.model.ContentPublishEvent;
import io.meeds.news.model.News;
import io.meeds.news.service.NewsTargetingService;
import jakarta.annotation.PostConstruct;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.exoplatform.commons.utils.CommonsUtils;
import org.exoplatform.services.listener.Asynchronous;
import org.exoplatform.services.listener.Event;
import org.exoplatform.services.listener.Listener;
import org.exoplatform.services.listener.ListenerService;
import org.exoplatform.social.core.manager.IdentityManager;
import org.exoplatform.social.core.space.model.Space;
import org.exoplatform.social.core.space.spi.SpaceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static io.meeds.analytics.utils.AnalyticsUtils.addSpaceStatistics;
import static io.meeds.news.utils.NewsUtils.*;

@Asynchronous
@Component
@Profile("analytics")
public class ContentPublishListener extends Listener<String, ContentPublishEvent> {

  private final SpaceService    spaceService;

  private final ListenerService listenerService;
  
  private final IdentityManager identityManager;
  
  private final NewsTargetingService newsTargetingService;

  private static String         CREATE_PUBLISH_CONTENT = "createPublishContent";

  private static String         UPDATE_PUBLISH_CONTENT = "updatePublishContent";

  private static final String[] LISTENER_EVENTS        = { CREATE_PUBLISH_CONTENT, UPDATE_PUBLISH_CONTENT };

  @Autowired
  public ContentPublishListener(SpaceService spaceService,
                                ListenerService listenerService,
                                IdentityManager identityManager,
                                NewsTargetingService newsTargetingService) {
    this.spaceService = spaceService;
    this.listenerService = listenerService;
    this.identityManager = identityManager;
    this.newsTargetingService = newsTargetingService;
  }

  @PostConstruct
  public void init() {
    for (String listener : LISTENER_EVENTS) {
      listenerService.addListener(listener, this);
    }
  }

  @Override
  public void onEvent(Event<String, ContentPublishEvent> event) throws Exception {
    String userName = event.getSource();
    long userId = Long.parseLong(identityManager.getOrCreateUserIdentity(userName).getId());
    News originalArticle = event.getData().getOriginalArticle();
    News updatedArticle = event.getData().getUpdatedArticle();
    if (event.getEventName().equals(CREATE_PUBLISH_CONTENT) || isValidPublish(originalArticle, updatedArticle)) {
      addPublishContentStatistics(userId, updatedArticle);
    }
  }

  private void addPublishContentStatistics(long userId, News news) {
    StatisticData statisticData = new StatisticData();

    statisticData.setModule("contents");
    statisticData.setSubModule("contents");
    statisticData.setOperation("publishContent");
    statisticData.setUserId(userId);
    statisticData.addParameter("contentTitle", news.getTitle());
    statisticData.addParameter("contentType", "News");
    statisticData.addParameter("contentCreator", news.getAuthor());
    statisticData.addParameter("contentPublishingTargets", toTargetNames(news));
    statisticData.addParameter("contentFeedPublishing", news.isActivityPosted() ? "YES" : "NO");
    String scheduleDates = toScheduleDates(news);
    if (StringUtils.isNotBlank(scheduleDates)) {
      statisticData.addParameter("contentScheduling", scheduleDates);
    }
    statisticData.addParameter("contentHideAuthor", news.getProperties().isHideAuthor() ? "YES" : "NO");
    statisticData.addParameter("contentHideReaction", news.getProperties().isHideReaction() ? "YES" : "NO");

    processSpaceStatistics(statisticData, news);

    AnalyticsUtils.addStatisticData(statisticData);
  }

  private List<String> toTargetNames(News news) {
    List<ArticleTarget> targets = newsTargetingService.getTargetsByNews(news);
    if (CollectionUtils.isEmpty(targets)) {
      return new ArrayList<>();
    }
    return targets.stream().map(ArticleTarget::getName).toList();
  }

  private String toScheduleDates(News news) {
    return Arrays.stream(new String[] { news.getSchedulePostDate(), news.getScheduleUnpublishDate() })
                 .filter(Objects::nonNull)
                 .collect(Collectors.joining(", "));
  }

  private boolean isValidPublish(News originalArticle, News updatedArticle) {
    return !originalArticle.isPublished() && updatedArticle.isPublished()
        || !originalArticle.isActivityPosted() && updatedArticle.isActivityPosted()
        || targetListUpdated(originalArticle.getTargets(), updatedArticle.getTargets())
        || !toScheduleDates(originalArticle).equals(toScheduleDates(updatedArticle))
        || isHideAuthorUpdated(originalArticle, updatedArticle) || isHideReactionUpdated(originalArticle, updatedArticle);
  }

  private boolean isHideReactionUpdated(News originalArticle, News updatedArticle) {
    if (originalArticle.getProperties() == null || updatedArticle.getProperties() == null) {
      return false;
    }
    return originalArticle.getProperties().isHideReaction() != updatedArticle.getProperties().isHideReaction();
  }

  private boolean isHideAuthorUpdated(News originalArticle, News updatedArticle) {
    if (originalArticle.getProperties() == null || updatedArticle.getProperties() == null) {
      return false;
    }
    return originalArticle.getProperties().isHideAuthor() != updatedArticle.getProperties().isHideAuthor();
  }

  private boolean targetListUpdated(List<ArticleTarget> targets, List<ArticleTarget> updatedTargets) {
    return !Objects.equals(extractTargetNames(targets), extractTargetNames(updatedTargets));
  }

  private Set<String> extractTargetNames(List<ArticleTarget> targets) {
    return targets == null ? Collections.emptySet() : targets.stream().map(ArticleTarget::getName).collect(Collectors.toSet());
  }
  
  private void processSpaceStatistics(StatisticData statisticData, News news) {
    Space space = spaceService.getSpaceById(news.getSpaceId());
    if (space != null) {
      addSpaceStatistics(statisticData, space);
    }
  }
}