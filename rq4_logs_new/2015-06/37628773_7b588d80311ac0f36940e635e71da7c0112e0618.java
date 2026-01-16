package com.lrgoncalves.product.catalog.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lrgoncalves.product.catalog.service.repositories.EventStore;
import com.lrgoncalves.product.catalog.service.repositories.EventStoreBuilder;
import com.sun.grizzly.http.SelectorThread;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.container.grizzly.GrizzlyWebContainerFactory;
import com.sun.syndication.feed.WireFeed;
import com.sun.syndication.feed.atom.Feed;
import com.sun.syndication.feed.atom.Link;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.WireFeedInput;

public class IntegrationTest {
    public static final String SERVER_URI = "http://localhost:8080/restbucks-service/service/";//"http://localhost:9998/";
    public static final String SERVICE_URI = SERVER_URI + "product-catalog/notifications";
    private static final String ATOM_MEDIA_TYPE = "application/atom+xml";

    private SelectorThread threadSelector;

    @Before
    public void startServer() throws Exception {
        EventStore.current().clear();
        final HashMap<String, String> initParams = new HashMap<String, String>();
        initParams.put("com.sun.jersey.config.property.packages", "com.lrgoncalves.product.catalog.service.resources");

        threadSelector = GrizzlyWebContainerFactory.create(SERVER_URI, initParams);
    }

    @After
    public void stopServer() {
        threadSelector.stopEndpoint();
    }

    @Test
    public void shouldGet404OnInvalidUri() throws Exception {
        URI badUri = new URI(SERVICE_URI + "/13,15");
        Client client = Client.create();
        ClientResponse response = client.resource(badUri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeAbleToGetTheRecentFeed() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(199);
        URI uri = new URI(SERVICE_URI + "/recent");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new StringReader(responseString));
        
        assertEquals(19, feed.getEntries().size());
    }

    @Test
    public void shouldBeAbleToGetAValidFeed() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(199);
        URI uri = new URI(SERVICE_URI + "/20,39");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new StringReader(responseString));

        assertEquals(20, feed.getEntries().size());
    }

    @Test
    public void shouldBeAbleToFindPrevLinkOnlyInRecentFeed() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(199);
        URI uri = new URI(SERVICE_URI + "/recent");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        WireFeedInput wfi = new WireFeedInput();
        WireFeed wireFeed = wfi.build(new StringReader(responseString));

        Feed atomFeed = (Feed) wireFeed;

        assertTrue(hasRelLink("prev-archive", atomFeed.getOtherLinks()));
        assertFalse(hasRelLink("next-archive", atomFeed.getOtherLinks()));
    }

    @Test
    public void shouldBeAbleToFindNextAndPrevLinksInGeneralFeed() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(199);
        URI uri = new URI(SERVICE_URI + "/40,59");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        WireFeedInput wfi = new WireFeedInput();
        WireFeed wireFeed = wfi.build(new StringReader(responseString));

        Feed atomFeed = (Feed) wireFeed;

        assertTrue(hasRelLink("prev-archive", atomFeed.getOtherLinks()));
        assertTrue(hasRelLink("next-archive", atomFeed.getOtherLinks()));
    }

    @Test
    public void shouldBeAbleToFindNextLinkOnlyInGeneralFeed() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(199);
        URI uri = new URI(SERVICE_URI + "/0,19");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        WireFeedInput wfi = new WireFeedInput();
        WireFeed wireFeed = wfi.build(new StringReader(responseString));

        Feed atomFeed = (Feed) wireFeed;

        assertFalse(hasRelLink("prev-archive", atomFeed.getOtherLinks()));
        assertTrue(hasRelLink("next-archive", atomFeed.getOtherLinks()));
    }
	
	@Test
	public void shouldFailToRetrieveFeedThatFallsOutsideOfFeedLengthBoundaries() throws Exception {
		EventStoreBuilder.eventStore().withRandomEvents(100);
        URI uri = new URI(SERVICE_URI + "/3,22");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);
		
        assertEquals(404, response.getStatus());
	}

    @Test
    public void shouldBeAbleToNavigateBackAndForthThroughTheFeeds() throws Exception {
        EventStoreBuilder.eventStore().withRandomEvents(105);

        URI uri = new URI(SERVICE_URI + "/recent");
        Client client = Client.create();
        ClientResponse response = client.resource(uri).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);

        assertEquals(200, response.getStatus());

        String responseString = response.getEntity(String.class);

        WireFeedInput wfi = new WireFeedInput();
        WireFeed wireFeed = wfi.build(new StringReader(responseString));

        Feed feed = (Feed) wireFeed;

        while (getPrevLink(feed) != null) {
            response = client.resource(getPrevLink(feed).getHref()).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);
            responseString = response.getEntity(String.class);
            wfi = new WireFeedInput();
            wireFeed = wfi.build(new StringReader(responseString));
            feed = (Feed) wireFeed;
        }
        
        // Make sure we're at the oldest feed (self = 0,19)
        Link selfLink = getSelfLink(feed);
        assertTrue(selfLink.getHref().endsWith("0,19"));        
        
        // Move forwards
        
        while(getNextLink(feed) != null) {
            response = client.resource(getNextLink(feed).getHref()).accept(ATOM_MEDIA_TYPE).get(ClientResponse.class);
            responseString = response.getEntity(String.class);
            wfi = new WireFeedInput();
            wireFeed = wfi.build(new StringReader(responseString));
            feed = (Feed) wireFeed;
        }
        
        // Make sure we're at the newest feed (self = 100, 119 or 105?)
        selfLink = getSelfLink(feed);
        assertTrue(selfLink.getHref().endsWith("100,119"));         
    }

    private Link getNextLink(Feed feed) {
        if (hasRelLink("next-archive", feed.getOtherLinks())) {
            for (Object obj : feed.getOtherLinks()) {
                Link l = (Link) obj;
                if (l.getRel().equals("next-archive")) {
                    return l;
                }
            }
        }
        return null;
    }

    private Link getSelfLink(Feed feed) {
        if (hasRelLink("self", feed.getOtherLinks())) {
            for (Object obj : feed.getOtherLinks()) {
                Link l = (Link) obj;
                if (l.getRel().equals("self")) {
                    return l;
                }
            }
        }
        return null;
    }

    private Link getPrevLink(Feed feed) {
        if (hasRelLink("prev-archive", feed.getOtherLinks())) {
            for (Object obj : feed.getOtherLinks()) {
                Link l = (Link) obj;
                if (l.getRel().equals("prev-archive")) {
                    return l;
                }
            }
        }
        return null;
    }

    private boolean hasRelLink(String relValue, List<Link> otherLinks) {
        for (Link l : otherLinks) {
            if (l.getRel().equals(relValue)) {
                return true;
            }
        }
        return false;
    }
}