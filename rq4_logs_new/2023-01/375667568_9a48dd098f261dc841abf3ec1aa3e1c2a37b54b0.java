package uk.gov.hmcts.reform.pip.subscription.management.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import uk.gov.hmcts.reform.pip.subscription.management.models.Channel;
import uk.gov.hmcts.reform.pip.subscription.management.models.Subscription;
import uk.gov.hmcts.reform.pip.subscription.management.models.external.data.management.Artefact;
import uk.gov.hmcts.reform.pip.subscription.management.models.external.publication.services.ThirdPartySubscription;
import uk.gov.hmcts.reform.pip.subscription.management.models.external.publication.services.ThirdPartySubscriptionArtefact;
import uk.gov.hmcts.reform.pip.subscription.management.repository.SubscriptionRepository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static uk.gov.hmcts.reform.pip.model.LogBuilder.writeLog;
import static uk.gov.hmcts.reform.pip.subscription.management.models.SearchType.CASE_ID;
import static uk.gov.hmcts.reform.pip.subscription.management.models.SearchType.CASE_URN;
import static uk.gov.hmcts.reform.pip.subscription.management.models.SearchType.LIST_TYPE;
import static uk.gov.hmcts.reform.pip.subscription.management.models.SearchType.LOCATION_ID;
import static uk.gov.hmcts.reform.pip.subscription.management.models.external.data.management.Sensitivity.CLASSIFIED;

@Service
@Slf4j
public class SubscriptionNotificationService {
    private static final String CASE_NUMBER_KEY = "caseNumber";
    private static final String CASE_URN_KEY = "caseUrn";

    @Autowired
    SubscriptionRepository repository;

    @Autowired
    AccountManagementService accountManagementService;

    @Autowired
    ChannelManagementService channelManagementService;

    @Autowired
    PublicationServicesService publicationServicesService;

    /**
     * Collect all subscribers for the artefact, and handle sending of email and third party subscriptions to
     * the subscribers.
     * @param artefact the artefact to collect the subscriptions for.
     */
    @Async
    public void collectSubscribers(Artefact artefact) {

        List<Subscription> subscriptionList = new ArrayList<>(querySubscriptionValueForLocation(
            LOCATION_ID.name(), artefact.getLocationId(), artefact.getListType().toString()));

        subscriptionList.addAll(querySubscriptionValue(LIST_TYPE.name(), artefact.getListType().name()));

        if (artefact.getSearch().containsKey("cases")) {
            artefact.getSearch().get("cases").forEach(object -> subscriptionList.addAll(extractSearchValue(object)));
        }

        List<Subscription> subscriptionsToContact = CLASSIFIED.equals(artefact.getSensitivity())
            ? validateSubscriptionPermissions(subscriptionList, artefact)
            : subscriptionList;

        handleSubscriptionSending(artefact.getArtefactId(), subscriptionsToContact);
    }

    /**
     * Collect the third party subscribers for the deleted artefact, and handle sending of notification emails to them.
     * @param artefactBeingDeleted the artefact which has been deleted.
     */
    @Async
    public void collectThirdPartyForDeletion(Artefact artefactBeingDeleted) {
        List<Subscription> subscriptionList = new ArrayList<>(querySubscriptionValue(
            LIST_TYPE.name(), artefactBeingDeleted.getListType().name()));

        List<Subscription> subscriptionsToContact = CLASSIFIED.equals(artefactBeingDeleted.getSensitivity())
            ? validateSubscriptionPermissions(subscriptionList, artefactBeingDeleted)
            : subscriptionList;

        handleDeletedArtefactSending(subscriptionsToContact, artefactBeingDeleted);
    }

    private List<Subscription> validateSubscriptionPermissions(List<Subscription> subscriptions, Artefact artefact) {
        List<Subscription> filteredList = new ArrayList<>();
        subscriptions.forEach(subscription -> {
            if (accountManagementService.isUserAuthorised(subscription.getUserId(), artefact.getListType(),
                                                          artefact.getSensitivity())) {
                filteredList.add(subscription);
            }
        });
        return filteredList;
    }

    private List<Subscription> querySubscriptionValue(String term, String value) {
        return repository.findSubscriptionsBySearchValue(term, value);
    }

    private List<Subscription> querySubscriptionValueForLocation(String term, String value, String listType) {
        return repository.findSubscriptionsByLocationSearchValue(term, value, listType);
    }

    @SuppressWarnings("unchecked")
    private List<Subscription> extractSearchValue(Object caseObject) {
        List<Subscription> subscriptionList = new ArrayList<>();
        Map<String, Object> caseMap = (LinkedHashMap) caseObject;

        if (caseMap.containsKey(CASE_NUMBER_KEY) && caseMap.get(CASE_NUMBER_KEY) != null) {
            subscriptionList.addAll(querySubscriptionValue(CASE_ID.name(), caseMap.get(CASE_NUMBER_KEY).toString()));
        }

        if (caseMap.containsKey(CASE_URN_KEY) && caseMap.get(CASE_URN_KEY) != null) {
            subscriptionList.addAll(querySubscriptionValue(CASE_URN.name(), caseMap.get(CASE_URN_KEY).toString()));
        }

        if (!caseMap.containsKey(CASE_NUMBER_KEY) || !caseMap.containsKey(CASE_URN_KEY)) {
            log.warn(writeLog(String.format("No value found in %s for case number or urn", caseObject)));
        }
        return subscriptionList;
    }

    /**
     * Handle forming and sending of subscriptions to publication services.
     *
     * @param artefactId The id of the artefact being sent
     * @param subscriptionsList The list of subscriptions being sent
     */
    private void handleSubscriptionSending(UUID artefactId, List<Subscription> subscriptionsList) {
        List<Subscription> emailList = sortSubscriptionByChannel(subscriptionsList, Channel.EMAIL.notificationRoute);
        List<Subscription> apiList = sortSubscriptionByChannel(subscriptionsList,
                                                               Channel.API_COURTEL.notificationRoute);

        channelManagementService.getMappedEmails(emailList).forEach((email, listOfSubscriptions) -> {
            log.info(writeLog("Summary being sent to publication services for id " + artefactId));
            publicationServicesService.postSubscriptionSummaries(artefactId, email, listOfSubscriptions);
        });

        channelManagementService.getMappedApis(apiList)
            .forEach((api, subscriptions) -> log.info(writeLog(
                publicationServicesService.sendThirdPartyList(new ThirdPartySubscription(api, artefactId)))));
        log.info(writeLog(String.format("Collected %s api subscribers", apiList.size())));
    }

    /**
     * Create a list of subscriptions for the correct channel.
     *
     * @param subscriptionsList The list of subscriptions to sort through
     * @param channel The channel we want the subscriptions of
     * @return A list of subscriptions
     */
    private List<Subscription> sortSubscriptionByChannel(List<Subscription> subscriptionsList, String channel) {
        List<Subscription> sortedSubscriptionsList = new ArrayList<>();

        subscriptionsList.forEach((Subscription subscription) -> {
            if (channel.equals(subscription.getChannel().notificationRoute)) {
                sortedSubscriptionsList.add(subscription);
            }
        });

        return sortedSubscriptionsList;
    }

    private void handleDeletedArtefactSending(List<Subscription> subscriptions, Artefact artefactBeingDeleted) {
        List<Subscription> apiList = sortSubscriptionByChannel(subscriptions,
                                                               Channel.API_COURTEL.notificationRoute);
        channelManagementService.getMappedApis(apiList)
            .forEach((api, subscription) -> log.info(writeLog(publicationServicesService.sendEmptyArtefact(
                new ThirdPartySubscriptionArtefact(api, artefactBeingDeleted)
            ))));
    }
}