package net.vivialconnect.model.message;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BulkInfo {

    @JsonProperty(value = "bulk_id")
    private String bulkId;
    @JsonProperty(value="total_message")
    private int totalMessage;
    @JsonProperty(value="date_created")
    private Date dateCreated;
    @JsonProperty(value="processed")
    private int processed;
    @JsonProperty(value="errors")
    private int errors;

    public String getBulkId() {
        return bulkId;
    }

    public void setBulkId(String bulkId) {
        this.bulkId = bulkId;
    }

    public int getTotalMessage() {
        return totalMessage;
    }

    public void setTotalMessage(int totalMessage) {
        this.totalMessage = totalMessage;
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    public int getProcessed() {
        return processed;
    }

    public void setProcessed(int processed) {
        this.processed = processed;
    }

    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

}