"use strict";

/*================================================
	PubSubHub: Publish and Subscribe exchange.

	Publishers:
		Issue data about a Topic to registered Subscribers.
	Subscribers:
		Register their interest in receiving data regarding 1 or more Topics.
	Topic:
		A Topic comprises a Data object and a list of Subscribers.
		When data is published to a Topic it is sent to all registered Subscribers.

--------------------------------------------------

subscribe(SUB, TOPIC_LIST, CALLBACK)
	Traverse each entry (TOPIC) in the TOPIC_LIST provided.
		Create the TOPIC if it does not already exist.
	Add the subscriber (CALLBACK) to the list on subscribers to to Topic, using the name SUB.
	If the is Data available for the Topic,
		invoke the Callback function to issue the Data to the subscriber.

unsubscribe(SUB, TOPIC_LIST)
	Traverse each entry (TOPIC) in the TOPIC_LIST provided.
		If the TOPIC exists and the subscriber (SUB) is listed,
			remove the entry from the list.

publish(TOPIC, DATA)
	Create the TOPIC if it does not already exist.
	Set the Topic's Data to the DATA to be published.
	If DATA has been supplied (not empty),
		traverse the list of subscribers and,
		invoke the Callback function to issue the Data to the subscriber.

================================================*/

var PubSubHub = (function() {
	var objTopics= {};

	function PubSubHub_subscribe(pSubId, pTopics, pCallback) {
		var topic,
			topicIndex,
			topicLoop;

		for (topicLoop=pTopics.length; topicLoop;) {
			topicIndex = pTopics[topicLoop-=1];
			topic = objTopics[topicIndex];

			if (!topic) {
				topic = (objTopics[topicIndex] = { data:null, subscribers:{}});
			}
			if (!!pCallback) {
				topic.subscribers[pSubId] = pCallback;
				if (!!topic.data) {
					pCallback(pSubId, topicIndex, topic.data);
				}
			}
		}
	};

	function PubSubHub_unsubscribe(pSubId, pTopics) {
		var topic,
			topicIndex,
			topicLoop;

		for (topicLoop=pTopics.length; topicLoop;) {
			topicIndex = pTopics[topicLoop-=1];
			topic = objTopics[topicIndex];

			if (!!objTopics[topicIndex].subscribers[pSubId]) {
				delete objTopics[topicIndex].subscribers[pSubId];
			}
		}
	};

	function PubSubHub_publish(pTopic, pData) {
		var topic = objTopics[pTopic],
			topicLoop;
		if (!topic) {
			topic = (objTopics[pTopic] = { data:null, subscribers:{}});
		}
		topic.data = pData;
		if (!!pData) {
			for (topicLoop in topic.subscribers) {
				topic.subscribers[topicLoop](topicLoop, pTopic, pData);
			}
		}
	};

	/* Publish the interface to the API */
	return {
		subscribe:PubSubHub_subscribe,
		unsubscribe:PubSubHub_unsubscribe,
		publish:PubSubHub_publish};
})();