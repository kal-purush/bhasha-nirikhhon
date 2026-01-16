package uk.gov.hmcts.reform.workallocation.email;

import org.apache.velocity.VelocityContext;
import uk.gov.hmcts.reform.workallocation.model.Task;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

public class TaskVelocityContext implements TypedVelocityContext<Task> {

    @Override
    public VelocityContext getVelocityContext(Task task, String deeplinkBaseUrl) {
        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("jurisdiction", task.getJurisdiction());
        velocityContext.put("lastModifiedDate", task.getLastModifiedDate());
        velocityContext.put("deepLinkUrl", deeplinkBaseUrl + task.getJurisdiction()
            +  "/" + task.getCaseTypeId() + "/" + task.getId());
        return velocityContext;
    }

    @Override
    public String getTemplateFileName(Task task) {
        return getJurisdiction(task) + ".vm";
    }

    @Override
    public void setMimeMessageSubject(Task task, MimeMessage msg) throws MessagingException {
        msg.setSubject(task.getId() + " - " + task.getState() + " - " + getJurisdiction(task).toUpperCase(),
            "UTF-8");
    }

    private String getJurisdiction(Task task) {
        return task.getJurisdiction() != null ? task.getJurisdiction().toLowerCase() : "divorce";
    }
}