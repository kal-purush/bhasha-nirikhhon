package myapp;

import com.google.api.server.spi.config.Api;
import com.google.api.server.spi.config.ApiMethod;

// _ah/api/buttonizer/v0.0.0.1/bpreading/1
@Api(name = "buttonizer",
        version = "v0.0.0.1",
        description = "Buttonizer Api")
public class ButtonizerEndpoint {
    @ApiMethod(
            name = "event.fire",
            path = "fire",
            httpMethod = ApiMethod.HttpMethod.POST)
    public void add(ButtonizerEvent event) {
        System.out.println("Event fired: " + event);
    }

    @ApiMethod(
            name = "event.pair",
            path = "pair",
            httpMethod = ApiMethod.HttpMethod.POST)
    public void pair(ButtonizerEvent event) {
        System.out.println("Event fired: " + event);
    }
}