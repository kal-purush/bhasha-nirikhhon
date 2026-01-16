package uk.gov.hmcts.reform.workallocation.email;

import org.apache.velocity.VelocityContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import uk.gov.hmcts.reform.workallocation.model.Task;

import java.time.LocalDateTime;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

public class TaskVelocityContextTest {

    private TaskVelocityContext context;
    private Task task;

    @Before
    public void setUp() {
        this.context = new TaskVelocityContext();
        this.task = Task.builder()
            .caseTypeId("DIVORCE")
            .id("123456789")
            .jurisdiction("DIVORCE")
            .state("Submitted")
            .lastModifiedDate(LocalDateTime.now())
            .build();
    }

    @Test
    public void testGetVelocityContext() {
        VelocityContext velocityContext = this.context.getVelocityContext(this.task, "deep_link_url/");
        Assert.assertEquals("DIVORCE", velocityContext.internalGet("jurisdiction"));
        Assert.assertEquals("deep_link_url/DIVORCE/DIVORCE/123456789", velocityContext.internalGet("deepLinkUrl"));
    }

    @Test
    public void testGetTemplateFileName() {
        String templateFileName = this.context.getTemplateFileName(task);
        Assert.assertEquals("divorce.vm", templateFileName);
    }

    @Test
    public void testGetTemplateFileNameWhenJurisdictionIsNull() {
        task.setJurisdiction(null);
        String templateFileName = this.context.getTemplateFileName(task);
        Assert.assertEquals("divorce.vm", templateFileName);
    }

    @Test
    public void testSetMimeMessageSubject() throws MessagingException {
        MimeMessage message = Mockito.mock(MimeMessage.class);
        this.context.setMimeMessageSubject(task, message);
        Mockito.verify(message, Mockito.times(1))
            .setSubject("123456789 - Submitted - DIVORCE", "UTF-8");
    }
}