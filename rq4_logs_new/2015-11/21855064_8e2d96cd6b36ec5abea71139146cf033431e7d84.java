package csw.opcDemo.hcd2;

import com.digitalpetri.opcua.sdk.client.OpcUaClient;
import com.digitalpetri.opcua.sdk.client.api.config.OpcUaClientConfig;
import com.digitalpetri.opcua.sdk.client.api.identity.AnonymousProvider;
import com.digitalpetri.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import com.digitalpetri.opcua.sdk.client.api.subscriptions.UaSubscription;
import com.digitalpetri.opcua.stack.client.UaTcpStackClient;
import com.digitalpetri.opcua.stack.core.AttributeId;
import com.digitalpetri.opcua.stack.core.security.SecurityPolicy;
import com.digitalpetri.opcua.stack.core.types.builtin.DataValue;
import com.digitalpetri.opcua.stack.core.types.builtin.LocalizedText;
import com.digitalpetri.opcua.stack.core.types.builtin.NodeId;
import com.digitalpetri.opcua.stack.core.types.builtin.QualifiedName;
import com.digitalpetri.opcua.stack.core.types.builtin.StatusCode;
import com.digitalpetri.opcua.stack.core.types.builtin.Variant;
import com.digitalpetri.opcua.stack.core.types.builtin.unsigned.UInteger;
import com.digitalpetri.opcua.stack.core.types.enumerated.MonitoringMode;
import com.digitalpetri.opcua.stack.core.types.enumerated.TimestampsToReturn;
import com.digitalpetri.opcua.stack.core.types.structured.EndpointDescription;
import com.digitalpetri.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import com.digitalpetri.opcua.stack.core.types.structured.MonitoringParameters;
import com.digitalpetri.opcua.stack.core.types.structured.ReadValueId;
import com.google.common.collect.ImmutableList;
import csw.opc.server.Hcd2Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.digitalpetri.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static com.google.common.collect.Lists.newArrayList;

/**
 * Provides client access to HCD's OPC UA server
 */
public class Hcd2OpcUaClient {
    private static final int NAMESPACE = 2;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final KeyStoreLoader loader = new KeyStoreLoader();
    private final AtomicLong clientHandles = new AtomicLong(1L);
    private final OpcUaClient client;


    public Hcd2OpcUaClient() throws Exception {
        client = createClient();
        // synchronous connect
        client.connect().get();  // FIXME
    }

    private OpcUaClient createClient() throws Exception {
        SecurityPolicy securityPolicy = SecurityPolicy.None;

        String endpointUrl = "opc.tcp://localhost:12685";
        EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints(endpointUrl).get(); // FIXME

        EndpointDescription endpoint = Arrays.stream(endpoints)
                .filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getSecurityPolicyUri()))
                .findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));

        logger.info("Using endpoint: {} [{}]", endpoint.getEndpointUrl(), securityPolicy);

        loader.load();

        OpcUaClientConfig config = OpcUaClientConfig.builder()
                .setApplicationName(LocalizedText.english("HCD2 opc-ua client"))
                .setApplicationUri("urn:hcd2:opcua:client")
                .setCertificate(loader.getClientCertificate())
                .setKeyPair(loader.getClientKeyPair())
                .setEndpoint(endpoint)
                .setIdentityProvider(new AnonymousProvider())
                .setRequestTimeout(uint(5000))
                .build();

        return new OpcUaClient(config);
    }

    public void subscribe(String name) throws Exception {

        // create a subscription and a monitored item
        UaSubscription subscription = client.getSubscriptionManager().createSubscription(1000.0).get(); // FIXME

        NodeId nodeId = new NodeId(NAMESPACE, Hcd2Namespace.NAMESPACE_PREFIX + name);

        ReadValueId readValueId = new ReadValueId(nodeId,
                AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);

        // client handle must be unique per item
        UInteger clientHandle = uint(clientHandles.getAndIncrement());

        MonitoringParameters parameters = new MonitoringParameters(
                clientHandle,
                1000.0,     // sampling interval
                null,       // filter, null means use default
                uint(10),   // queue size
                true);      // discard oldest

        MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                readValueId, MonitoringMode.Reporting, parameters);

        List<UaMonitoredItem> items = subscription
                .createMonitoredItems(TimestampsToReturn.Both, newArrayList(request)).get(); // FIXME

        // do something with the value updates
        UaMonitoredItem item = items.get(0);

        item.setValueConsumer(v -> {
            logger.info("HCD {} subscriber: value received: {}", name, v.getValue());
        });
    }

    public void setValue(String name, Object value) throws Exception  {
        Variant v = new Variant(value);

        // don't write status or timestamps
        DataValue dv = new DataValue(v, null, null);

        NodeId nodeId = new NodeId(NAMESPACE, Hcd2Namespace.NAMESPACE_PREFIX + name);
        List<NodeId> nodeIds = newArrayList(nodeId);

        // write asynchronously....
        CompletableFuture<List<StatusCode>> f =
                client.writeValues(nodeIds, ImmutableList.of(dv));

        // ...but block for the results so we write in order
        List<StatusCode> statusCodes = f.get(); // FIXME
        StatusCode status = statusCodes.get(0);

        if (status.isGood()) {
            logger.info("Wrote '{}' to nodeId={}", v, nodeIds.get(0));
        } else {
            logger.error("Write '{}' failed for nodeId={}", v, nodeIds.get(0));
        }
    }
}