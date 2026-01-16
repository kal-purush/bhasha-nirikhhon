package pl.com.softproject.utils.freshmail.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Class MultipleSubscriber.
 *
 * @author Marcin Jasiński {@literal <mkjasinski@gmail.com>}
 */
public class MultipleSubscriber implements Serializable {

    @NotNull
    private String email;

    @JsonProperty("custom_fields")
    private Map<String, String> fields = new LinkedHashMap<>();

    public MultipleSubscriber() {
    }

    public MultipleSubscriber(final String email) {
        this(email, Collections.<String, String>emptyMap());
    }

    public MultipleSubscriber(final String email, final Map<String, String> fields) {
        this.email = email;
        this.fields = fields;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(final String email) {
        this.email = email;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(final Map<String, String> fields) {
        this.fields = fields;
    }
}