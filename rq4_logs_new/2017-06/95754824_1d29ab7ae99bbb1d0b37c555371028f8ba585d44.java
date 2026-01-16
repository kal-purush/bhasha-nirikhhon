package org.vaadin.connectoridgenerator;

import java.util.HashSet;
import java.util.Set;
import com.vaadin.data.provider.DataCommunicator;
import com.vaadin.server.ConnectorIdGenerationEvent;
import com.vaadin.server.ConnectorIdGenerator;
import com.vaadin.shared.Connector;
import com.vaadin.ui.Component;
import com.vaadin.ui.components.grid.MultiSelectionModelImpl;
import com.vaadin.ui.components.grid.SingleSelectionModelImpl;

/**
 * Connector id generator which uses the component id to make connector ids
 * independent of the number of connectors (component) appearing elsewhere in
 * the application and view.
 * <p>
 * Note that this class should never be used in production as it keeps track of
 * the id of all used connectors in the session for the lifetime of the session.
 * <p>
 * It does not track different UIs separately, so you should not open multiple
 * UIs when using this class or the ids will depend on the order in which the
 * UIs have been loaded.
 */
public class ComponentIdBasedConnectorIdGenerator
        implements ConnectorIdGenerator {

    private Set<String> usedConnectorIds = new HashSet<>();

    @Override
    public String generateConnectorId(ConnectorIdGenerationEvent event) {
        String connectorId;
        Connector connector = event.getConnector();
        if (connector instanceof Component) {
            Component component = (Component) connector;

            String componentId = component.getId();
            if (componentId == null) {
                return ConnectorIdGenerator.generateDefaultConnectorId(event);
            } else if (!usedConnectorIds.contains(componentId)) {
                connectorId = componentId;
            } else {
                // prevent two accidentally identical connector ids
                connectorId = createNewId(componentId);
            }
        } else if (connector.getClass().equals(DataCommunicator.class)) {
            // treat separately since it's not possible set id for this
            connectorId = createNewId("datareqrpc");
        } else if (connector.getClass()
                .equals(SingleSelectionModelImpl.class)) {
            // treat separately since it's not possible set id for this
            connectorId = createNewId("singleselectrpc");
        } else if (connector.getClass().equals(MultiSelectionModelImpl.class)) {
            // treat separately since it's not possible set id for this
            connectorId = createNewId("multiselectrpc");
        } else {
            return ConnectorIdGenerator.generateDefaultConnectorId(event);
        }
        usedConnectorIds.add(connectorId);
        return connectorId;
    }

    private String createNewId(String id) {
        int count = 0;
        String modifiedId;
        do {
            modifiedId = id + count;
            count++;
        } while (usedConnectorIds.contains(modifiedId));
        return modifiedId;
    }

}