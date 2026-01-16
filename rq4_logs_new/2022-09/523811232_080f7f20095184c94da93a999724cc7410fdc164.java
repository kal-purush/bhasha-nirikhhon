package com.tobiakindele.ehr.server.app;

import com.tobiakindele.ehr.server.app.enums.TransactionType;
import com.tobiakindele.ehr.server.app.service.ContractService;
import com.tobiakindele.ehr.server.app.utils.EnrollAdmin;
import com.tobiakindele.ehr.server.app.utils.RegisterUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EhrServerAppApplication {

    private final static Logger logger = LoggerFactory.getLogger(EhrServerAppApplication.class);

    static {
        System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "true");
    }

    public static void main(String[] args) throws Exception {

        EnrollAdmin.init();
        RegisterUser.init();

        logger.info("Submit Transaction: InitLedger creates the initial set of assets on the ledger.");
        new ContractService().invokeTransaction("InitLedger", TransactionType.SUBMIT);

        SpringApplication.run(EhrServerAppApplication.class, args);
    }

}