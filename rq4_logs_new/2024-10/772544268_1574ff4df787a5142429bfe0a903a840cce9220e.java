package org.shikshalokam.backend.scp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;

public class SelfCreationPortalCreateNewPermission extends SelfCreationPortalBaseTest {
    private static final Logger logger = LogManager.getLogger(SelfCreationPortalCreateNewPermission.class);

    @BeforeTest
    public void init() {
        logger.info("Logging into the application :");
        loginToScp(PROP_LIST.get("scp.qa.admin.login.user").toString(), PROP_LIST.get("scp.qa.admin.login.password").toString());
    }
    @Test (description = "Verifies the functionality of creating new user's permission.")
    public void testCreateNewPermission(){

    }
}