
package net.jamie.datamodels.inforesponse;

import io.cucumber.datatable.dependency.com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "1027"
})
public class Data {

    @JsonProperty("1027")
    private _1027 _1027;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("1027")
    public _1027 get1027() {
        return _1027;
    }

    @JsonProperty("1027")
    public void set1027(_1027 _1027) {
        this._1027 = _1027;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}