package org.benbroadaway.unifi.actions.device;

import org.benbroadaway.unifi.UnifiDevice;
import org.benbroadaway.unifi.actions.AbstractAction;
import org.benbroadaway.unifi.actions.ActionResult;
import org.benbroadaway.unifi.actions.UnifiHttpClient;
import org.benbroadaway.unifi.client.ApiCredentials;

import java.util.List;
import java.util.concurrent.Callable;

public class GetDevices extends AbstractAction implements Callable<ActionResult<List<UnifiDevice>>> {

    public static GetDevices getInstance(String unifiHost,
                                         ApiCredentials unifiCreds,
                                         boolean validateCerts) {
        return new GetDevices(unifiHost, unifiCreds, validateCerts);
    }

    private GetDevices(String unifiHost, ApiCredentials creds, boolean validateCerts) {
        super(unifiHost, creds, validateCerts);
    }

    public GetDevices(UnifiHttpClient unifiClient) {
        super(unifiClient);
    }

    @Override
    public ActionResult<List<UnifiDevice>> call() {
        var devices = getDevices();

        return ActionResult.<List<UnifiDevice>>builder()
                .ok(true)
                .data(devices)
                .build();
    }
}