package org.benbroadaway.unifi.actions.usp;

import com.fasterxml.jackson.databind.JavaType;
import org.benbroadaway.unifi.Device;
import org.benbroadaway.unifi.OutletOverride;
import org.benbroadaway.unifi.actions.ApiResponse;
import org.benbroadaway.unifi.actions.UnifiHttpClient;
import org.benbroadaway.unifi.actions.UnifiResult;
import org.benbroadaway.unifi.actions.Util;
import org.benbroadaway.unifi.client.ApiCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.Callable;

public class RelayState implements Callable<UnifiResult<Boolean>> {
    private static final Logger log = LoggerFactory.getLogger(RelayState.class);

    private final String plugName;
    private final UnifiHttpClient unifiClient;

    public static RelayState get(String unifiHost, String plugName, ApiCredentials unifiCreds, boolean validateCerts) {
        return new RelayState(unifiHost, plugName, unifiCreds, validateCerts);
    }

    public RelayState(String unifiHost, String plugName, ApiCredentials unifiCreds, boolean validateCerts) {
        this.plugName = plugName;
        this.unifiClient = new UnifiHttpClient(unifiHost, unifiCreds, validateCerts);
    }

    @Override
    public UnifiResult<Boolean> call() throws Exception {
        var currentDevice = getCurrentDevice();
        var relayIndex = 1;
        var currentState = getOutletRelayState(currentDevice, relayIndex); // TODO handle multi-outlet devices

        return UnifiResult.<Boolean>builder()
                .ok(true)
                .data(currentState)
                .build();
    }

    private boolean getOutletRelayState(Device device, int outlet) {
        return device.outletOverrides().stream()
                .filter(oo -> oo.index() == outlet)
                .map(OutletOverride::relayState)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No outlet number: " + outlet));
    }

    private Device getCurrentDevice() {
        var uri = unifiClient.resolve("/proxy/network/api/s/default/stat/device");

        return unifiClient.withClient((client, csrfToken) -> {
            var req = HttpRequest.newBuilder()
                    .uri(uri)
                    .header("Accept", UnifiHttpClient.APPLICATION_JSON)
                    .header("x-csrf-token", csrfToken)
                    .GET()
                    .build();

            try {
                var resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());

                if (resp.statusCode() != 200) {
                    throw new IllegalStateException("invalid response code: ${resp.statusCode()}");
                }

                JavaType listOfDevicesType = Util.getMapper().getTypeFactory().constructCollectionType(List.class, Device.class);

                ApiResponse<List<Device>> apiResponse = readBody(resp, listOfDevicesType);
                return apiResponse.data().stream()
                        .filter(d -> d.model().equals("UP1"))
                        .filter(d -> d.name().equals(plugName))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No UP1 device found with name '" + plugName + "'"));
            } catch (Exception e) {
                throw new RuntimeException("Error retrieving current device state: " + e.getMessage());
            }
        });
    }
    private <T> ApiResponse<T> readBody(HttpResponse<InputStream> resp, JavaType returnParam) {
        try (var input = resp.body()) {
            return Util.withMapper(mapper -> {
                var t = mapper.getTypeFactory().constructParametricType(ApiResponse.class, returnParam);
                return mapper.readValue(input, t);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}