package uk.nhs.prm.e2etests.performance;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import uk.nhs.prm.e2etests.enumeration.Gp2GpSystem;
import uk.nhs.prm.e2etests.model.templatecontext.ContinueRequestTemplateContext;
import uk.nhs.prm.e2etests.model.templatecontext.EhrRequestTemplateContext;
import uk.nhs.prm.e2etests.queue.SimpleAmqpQueue;
import uk.nhs.prm.e2etests.queue.ehrtransfer.EhrTransferServiceParsingDeadLetterQueue;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceLargeEhrFragmentsOQ;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceLargeEhrOQ;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceNegativeAcknowledgementOQ;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceSmallEhrOQ;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceTransferCompleteOQ;
import uk.nhs.prm.e2etests.queue.ehrtransfer.observability.EhrTransferServiceUnhandledOQ;
import uk.nhs.prm.e2etests.queue.gp2gpmessenger.observability.Gp2GpMessengerOQ;
import uk.nhs.prm.e2etests.service.RepoService;
import uk.nhs.prm.e2etests.service.TemplatingService;
import uk.nhs.prm.e2etests.test.ForceXercesParserSoLogbackDoesNotBlowUpWhenUsingSwiftMqClient;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.nhs.prm.e2etests.enumeration.Gp2GpSystem.TPP_PTL_INT;
import static uk.nhs.prm.e2etests.enumeration.TemplateVariant.CONTINUE_REQUEST;
import static uk.nhs.prm.e2etests.enumeration.TemplateVariant.EHR_REQUEST;
import static uk.nhs.prm.e2etests.utility.ThreadUtility.sleepFor;

@Log4j2
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(ForceXercesParserSoLogbackDoesNotBlowUpWhenUsingSwiftMqClient.class)
public class RepositoryPerformanceTest {
    // TODO: Migrate all necessary tests from RepoInPerformanceTest here.

    private final RepoService repoService;
    private final TemplatingService templatingService;
    private final SimpleAmqpQueue mhsInboundQueue;
    private final Gp2GpMessengerOQ gp2gpMessengerOQ;
    private final EhrTransferServiceTransferCompleteOQ ehrTransferServiceTransferCompleteOQ;
    private final EhrTransferServiceUnhandledOQ ehrTransferServiceUnhandledOQ;
    private final EhrTransferServiceLargeEhrFragmentsOQ ehrTransferServiceLargeEhrFragmentsOQ;
    private final EhrTransferServiceSmallEhrOQ ehrTransferServiceSmallEhrOQ;
    private final EhrTransferServiceLargeEhrOQ ehrTransferServiceLargeEhrOQ;
    private final EhrTransferServiceNegativeAcknowledgementOQ ehrTransferServiceNegativeAcknowledgementOQ;
    private final EhrTransferServiceParsingDeadLetterQueue ehrTransferServiceParsingDeadLetterQueue;

    @Autowired
    public RepositoryPerformanceTest(
            RepoService repoService,
            TemplatingService templatingService,
            SimpleAmqpQueue mhsInboundQueue,
            Gp2GpMessengerOQ gp2gpMessengerOQ,
            EhrTransferServiceTransferCompleteOQ ehrTransferServiceTransferCompleteOQ,
            EhrTransferServiceUnhandledOQ ehrTransferServiceUnhandledOQ,
            EhrTransferServiceLargeEhrFragmentsOQ ehrTransferServiceLargeEhrFragmentsOQ,
            EhrTransferServiceSmallEhrOQ ehrTransferServiceSmallEhrOQ,
            EhrTransferServiceLargeEhrOQ ehrTransferServiceLargeEhrOQ,
            EhrTransferServiceNegativeAcknowledgementOQ ehrTransferServiceNegativeAcknowledgementOQ,
            EhrTransferServiceParsingDeadLetterQueue ehrTransferServiceParsingDeadLetterQueue
    ) {
        this.repoService = repoService;
        this.templatingService = templatingService;
        this.mhsInboundQueue = mhsInboundQueue;
        this.gp2gpMessengerOQ = gp2gpMessengerOQ;
        this.ehrTransferServiceTransferCompleteOQ = ehrTransferServiceTransferCompleteOQ;
        this.ehrTransferServiceUnhandledOQ = ehrTransferServiceUnhandledOQ;
        this.ehrTransferServiceLargeEhrFragmentsOQ = ehrTransferServiceLargeEhrFragmentsOQ;
        this.ehrTransferServiceSmallEhrOQ = ehrTransferServiceSmallEhrOQ;
        this.ehrTransferServiceLargeEhrOQ = ehrTransferServiceLargeEhrOQ;
        this.ehrTransferServiceNegativeAcknowledgementOQ = ehrTransferServiceNegativeAcknowledgementOQ;
        this.ehrTransferServiceParsingDeadLetterQueue = ehrTransferServiceParsingDeadLetterQueue;
    }

    @BeforeAll
    void init() {
        ehrTransferServiceSmallEhrOQ.deleteAllMessages();
        ehrTransferServiceLargeEhrOQ.deleteAllMessages();
        ehrTransferServiceLargeEhrFragmentsOQ.deleteAllMessages();
        ehrTransferServiceParsingDeadLetterQueue.deleteAllMessages();
        ehrTransferServiceTransferCompleteOQ.deleteAllMessages();
        ehrTransferServiceUnhandledOQ.deleteAllMessages();
        ehrTransferServiceNegativeAcknowledgementOQ.deleteAllMessages();
        gp2gpMessengerOQ.deleteAllMessages();
    }

    @Test
    void Given_SuperLargeEhrWith100Fragments_When_PutIntoRepoAndPulledOut_Then_SuccessfullyVisibleOnGp2gpMessengerOQ() {
        // given
        String patientNhsNumber = "9727018157";
        EhrRequestTemplateContext ehrRequestTemplateContext = EhrRequestTemplateContext.builder()
                .nhsNumber(patientNhsNumber)
                .newGpOdsCode(TPP_PTL_INT.odsCode())
                .asidCode(TPP_PTL_INT.asidCode()).build();
        String ehrRequestMessage = this.templatingService.getTemplatedString(EHR_REQUEST, ehrRequestTemplateContext);
        String outboundConversationId = ehrRequestTemplateContext.getOutboundConversationId();
        log.info("OutboundConversationID: " + outboundConversationId);
        ContinueRequestTemplateContext continueRequestTemplateContext =
                ContinueRequestTemplateContext.builder()
                        .outboundConversationId(outboundConversationId)
                        .recipientOdsCode(Gp2GpSystem.REPO_DEV.odsCode())
                        .senderOdsCode(TPP_PTL_INT.odsCode()).build();
        String continueRequestMessage = this.templatingService.getTemplatedString(CONTINUE_REQUEST, continueRequestTemplateContext);

        // when
        this.repoService.addLargeEhrWithVariableManifestToRepo(patientNhsNumber, 100, TPP_PTL_INT.odsCode());
        this.mhsInboundQueue.sendMessage(ehrRequestMessage, outboundConversationId);

        sleepFor( 10000);

        this.mhsInboundQueue.sendMessage(continueRequestMessage, outboundConversationId);

        boolean messagesExist = this.gp2gpMessengerOQ.getAllMessagesFromQueueWithConversationId(1,
                100,
                outboundConversationId);

        // then
        assertTrue(messagesExist);
    }
}