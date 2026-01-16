package co.uk.sentinelweb.bikemapper.net.response.mapquest;

import org.junit.Test;

import co.uk.sentinelweb.bikemapper.net.TestResourceUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;


/**
 * Created by robert on 29/06/16.
 */
public class MapQuestDirectionsResponseTest {

    @Test
    public void shouldTestMapquestResponse() {
        final String string = TestResourceUtils.getString(getClass(), "mapquest_response.json");
        final MapQuestDirectionsResponse body = TestResourceUtils.getGson().fromJson(string, MapQuestDirectionsResponse.class);
        checkResponse(body);
    }

    public static void checkResponse(final MapQuestDirectionsResponse body) {
        assertThat("response was null", body, is(notNullValue()));
        assertThat("response wasn't OK", body.info.status, is(0));
        assertThat("routes.locations shouldnt be null", body.route.locations, is(notNullValue()));
        assertThat("routes.locations  should be size 2", body.route.locations.size(), is(2));
        assertThat("routes.legs shouldnt be null", body.route.legs, is(notNullValue()));
        assertThat("routes.legs  should be size 1", body.route.legs.size(), is(1));
        assertThat("routes.legs[0].maneuvers shouldnt be null", body.route.legs.get(0).maneuvers, is(notNullValue()));
        assertThat("routes.legs[0].maneuvers shouldnt be null", body.route.legs.get(0).maneuvers.size(), is(17));
        assertThat("routes.legs[0].maneuvers[0].startpoint shouldnt be null", body.route.legs.get(0).maneuvers.get(0).startPoint, is(notNullValue()));

    }
}