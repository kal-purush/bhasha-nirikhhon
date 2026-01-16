package com.aspirine.global.abstracts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ektorp.support.CouchDbDocument;

import java.io.Serializable;

/**
 * Abstract class that has common property for all CouchDB documents
 *
 * @author yathish
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class MathDocument extends CouchDbDocument implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("math_code")
    private String mathCode;

    @JsonProperty("type")
    private String type;

    protected MathDocument(final String mathCode) {
        this.mathCode = mathCode;
    }

    public String getMathCode() {
        return mathCode;
    }

    public void setMathCode(String mathCode) {
        this.mathCode = mathCode;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}