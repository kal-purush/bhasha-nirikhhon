package io.managed.services.test.devexp;


import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequest;
import com.openshift.cloud.v1alpha.models.CloudServiceAccountRequestSpec;
import com.openshift.cloud.v1alpha.models.CloudServicesRequest;
import com.openshift.cloud.v1alpha.models.CloudServicesRequestSpec;
import com.openshift.cloud.v1alpha.models.Credentials;
import com.openshift.cloud.v1alpha.models.ServiceRegistryConnection;
import com.openshift.cloud.v1alpha.models.ServiceRegistryConnectionSpec;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.LogCollector;
import io.managed.services.test.operator.OperatorUtils;
import io.managed.services.test.wait.ReadyFunction;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.TestException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.cleanRegistry;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;


/**
 * <p>
 * The tests expect the operator to be already installed on the dev cluster, the dev cluster is given by
 * the DEV_CLUSTER_SERVER env. The tested CRs will be created in the DEV_CLUSTER_NAMESPACE set namespace.
 * <p>
 * The DEV_CLUSTER_NAMESPACE can and the DEV_CLUSTER_TOKEN should be created using
 * the ./hack/bootstrap-mk-e2e-tests-namespace.sh script.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-operator
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> DEV_CLUSTER_SERVER
 *     <li> DEV_CLUSTER_TOKEN
 * </ul>
 */
public class ServiceRegistryOperatorTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaOperatorTest.class);

    private KeycloakUser user;
    private SecurityMgmtApi securityMgmtApi;
    private OpenShiftClient oc;

    private CloudServicesRequest cloudServicesRequest;

    private final static String ACCESS_TOKEN_SECRET_NAME = "mk-e2e-api-accesstoken";
    private final static String CLOUD_SERVICE_ACCOUNT_REQUEST_NAME = "mk-e2e-service-account-request";
    private final static String SERVICE_ACCOUNT_NAME = "mk-e2e-bo-sa-" + Environment.LAUNCH_KEY;
    private final static String SERVICE_ACCOUNT_SECRET_NAME = "mk-e2e-service-account-secret";
    private final static String CLOUD_SERVICES_REQUEST_NAME = "mk-e2e-kafka-request";
    private final static String SERVICE_REGISTRY_CONNECTION_NAME = "mk-e2e-registry-connection";

    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-operator-sr-" + Environment.LAUNCH_KEY;
    private RegistryMgmtApi registryMgmtApi;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.DEV_CLUSTER_SERVER, "the DEV_CLUSTER_SERVER env is null");
        assertNotNull(Environment.DEV_CLUSTER_TOKEN, "the DEV_CLUSTER_TOKEN env is null");

        var auth = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);
        LOGGER.info("authenticate user '{}' against RH SSO", auth.getUsername());
        user = bwait(auth.loginToRedHatSSO());

        LOGGER.info("initialize security and register apis");
        securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, user);
        registryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.OPENSHIFT_API_URI, user);

        LOGGER.info("initialize openshift client");
        var config = new ConfigBuilder()
                .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
                .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
                .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
                .build();
        oc = new DefaultOpenShiftClient(config);

        LOGGER.info("apply service registry '{}'", SERVICE_REGISTRY_NAME);
        RegistryMgmtApiUtils.applyRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);

        try {
            OperatorUtils.patchTheOperatorCloudServiceAPIEnv(oc);
        } catch (Throwable t) {
            LOGGER.error("failed to patch the CLOUD_SERVICES_API env:", t);
        }
    }

    private void cleanAccessTokenSecret() {
        Secret s = oc.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            oc.secrets().delete(s);
        }
    }

    private void cleanCloudServiceAccountRequest() {
        var a = OperatorUtils.cloudServiceAccountRequest(oc).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get();
        if (a != null) {
            LOGGER.info("clean CloudServiceAccountRequest: {}", a.getMetadata().getName());
            OperatorUtils.cloudServiceAccountRequest(oc).delete(a);
        }
    }

    private void cleanCloudServicesRequest() {
        var k = OperatorUtils.cloudServicesRequest(oc).withName(CLOUD_SERVICES_REQUEST_NAME).get();
        if (k != null) {
            LOGGER.info("clean CloudServicesRequest: {}", k.getMetadata().getName());
            OperatorUtils.cloudServicesRequest(oc).delete(k);
        }
    }

    private void cleanServiceRegistryConnection() {
        var c = OperatorUtils.serviceRegistryConnection(oc).withName(SERVICE_REGISTRY_CONNECTION_NAME).get();
        if (c != null) {
            LOGGER.info("clean ServiceRegistryConnection: {}", c.getMetadata().getName());
            OperatorUtils.serviceRegistryConnection(oc).delete(c);
        }
    }

    private void collectOperatorLogs(ITestContext context) throws IOException {
        LogCollector.saveDeploymentLog(
                TestUtils.getLogPath(Environment.LOG_DIR.resolve("test-logs").toString(), context),
                oc,
                "openshift-operators",
                "service-binding-operator");

    }

    @AfterClass(alwaysRun = true)
    public void teardown(ITestContext context) {
        assumeTeardown();

        try {
            cleanServiceRegistryConnection();
        } catch (Exception e) {
            LOGGER.error("clean kafka connection error: ", e);
        }

        try {
            cleanCloudServicesRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud services request error: ", e);
        }

        try {
            cleanCloudServiceAccountRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud service account request error: ", e);
        }

        try {
            cleanAccessTokenSecret();
        } catch (Exception e) {
            LOGGER.error("clean access token secret error: ", e);
        }

        try {
            collectOperatorLogs(context);
        } catch (Exception e) {
            LOGGER.error("collect operator logs error: ", e);
        }

        // force clean the service account if it hasn't done it yet
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("cleanServiceAccount error: ", t);
        }

        try {
            cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

    }

    @Test
    public void testCreateAccessTokenSecret() {

        // Create Secret
        var data = new HashMap<String, String>();
        data.put("value", Base64.getEncoder().encodeToString(user.getRefreshToken().getBytes()));

        LOGGER.info("create access token secret with name: {}", ACCESS_TOKEN_SECRET_NAME);
        oc.secrets().create(OperatorUtils.buildSecret(ACCESS_TOKEN_SECRET_NAME, data));
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret")
    public void testCreateCloudServiceAccountRequest() throws Throwable {

        var a = new CloudServiceAccountRequest();
        a.getMetadata().setName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a.setSpec(new CloudServiceAccountRequestSpec());
        a.getSpec().setServiceAccountName(SERVICE_ACCOUNT_NAME);
        a.getSpec().setServiceAccountDescription("");
        a.getSpec().setServiceAccountSecretName(SERVICE_ACCOUNT_SECRET_NAME);
        a.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServiceAccountRequest with name: {}", CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a = OperatorUtils.cloudServiceAccountRequest(oc).create(a);
        LOGGER.info("created CloudServiceAccountRequest: {}", Json.encode(a));

        ReadyFunction<Void> ready = (__, ___) -> {
            var r = OperatorUtils.cloudServiceAccountRequest(oc).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get();
            LOGGER.debug(r);

            return r.getStatus() != null && r.getStatus().getMessage().equals("Created");
        };
        waitFor("CloudServiceAccountRequest to be created", ofSeconds(10), ofMinutes(4), ready);
        LOGGER.info("CloudServiceAccountRequest is created");
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret")
    public void testCreateCloudServicesRequest() throws Throwable {

        var k = new CloudServicesRequest();
        k.getMetadata().setName(CLOUD_SERVICES_REQUEST_NAME);
        k.setSpec(new CloudServicesRequestSpec());
        k.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServicesRequest with name: {}", CLOUD_SERVICES_REQUEST_NAME);
        k = OperatorUtils.cloudServicesRequest(oc).create(k);
        LOGGER.info("created CloudServicesRequest: {}", Json.encode(k));

        ReadyFunction<CloudServicesRequest> ready = (__, atom) -> {
            var r = OperatorUtils.cloudServicesRequest(oc).withName(CLOUD_SERVICES_REQUEST_NAME).get();
            LOGGER.debug(r);

            if (r.getStatus() != null
                    && r.getStatus().getServiceRegistries() != null
                    && !r.getStatus().getServiceRegistries().isEmpty()) {

                atom.set(r);
                return true;
            }
            return false;
        };
        cloudServicesRequest = waitFor("CloudServicesRequest to complete", ofSeconds(10), ofMinutes(5), ready);
        LOGGER.info("CloudServicesRequest is completed");
    }

    @Test(dependsOnMethods = {"testCreateCloudServiceAccountRequest", "testCreateCloudServicesRequest"})
    public void testCreateServiceRegistryConnection() throws Throwable {

        var serviceRegistry = cloudServicesRequest.getStatus().getServiceRegistries().stream()
                .filter(r -> r.getName().equals(SERVICE_REGISTRY_NAME))
                .findFirst();

        if (serviceRegistry.isEmpty()) {
            LOGGER.info("CloudServicesRequest: {}", Json.encode(cloudServicesRequest));
            throw new TestException(message("failed to find the user service registry {} in the CloudServicesRequest {}", SERVICE_REGISTRY_NAME, CLOUD_SERVICES_REQUEST_NAME));
        }

        var c = new ServiceRegistryConnection();

        c.getMetadata().setName(SERVICE_REGISTRY_CONNECTION_NAME);
        c.setSpec(new ServiceRegistryConnectionSpec());
        c.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);
        c.getSpec().setServiceRegistryId(serviceRegistry.orElseThrow().getId());
        c.getSpec().setCredentials(new Credentials(SERVICE_ACCOUNT_SECRET_NAME));

        LOGGER.info("create ManagedServiceRegistryConnection with name: {}", SERVICE_REGISTRY_CONNECTION_NAME);
        c = OperatorUtils.serviceRegistryConnection(oc).create(c);
        LOGGER.info("created ManagedServiceRegistryConnection: {}", Json.encode(c));

        ReadyFunction<Void> ready = (__, ___) -> {
            var r = OperatorUtils.serviceRegistryConnection(oc).withName(SERVICE_REGISTRY_CONNECTION_NAME).get();
            LOGGER.debug(r);

            return r.getStatus() != null
                    && r.getStatus().getMessage() != null
                    && r.getStatus().getMessage().equals("Created");
        };
        waitFor("ManagedServiceRegistryConnection to be created", ofSeconds(10), ofMinutes(2), ready);
        LOGGER.info("ManagedServiceRegistryConnection is created");
    }
}