package uk.gov.ida.rp.testrp.tokenservice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenDto {

    @Valid
    @NotNull
    @JsonProperty(value = "epoch", required = true)
    private int epoch;

    @Valid
    @NotNull
    @JsonProperty(value = "valid_until", required = true)
    private DateTime validUntil;

    @Valid
    @NotNull
    @JsonProperty(value = "issued_to", required = true)
    private String issuedTo;

    // all of these annotations are required, otherwise an error won't be thrown for missing fields
    @JsonCreator
    private TokenDto(@JsonProperty(value = "epoch", required = true) int epoch,
                     @JsonProperty(value = "valid_until", required = true) DateTime validUntil,
                     @JsonProperty(value = "issued_to", required = true) String issuedTo) {
        this.epoch = epoch;
        this.validUntil = validUntil;
        this.issuedTo = issuedTo;
    }

    public int getEpoch() {
        return epoch;
    }

    public DateTime getValidUntil() {
        return validUntil;
    }

    public String getIssuedTo() {
        return issuedTo;
    }
}