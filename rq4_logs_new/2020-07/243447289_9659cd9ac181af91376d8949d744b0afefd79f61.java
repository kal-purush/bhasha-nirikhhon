package de.aservo.confapi.commons.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Bean for user directory settings in REST requests.
 */
@Data
@NoArgsConstructor
@XmlRootElement
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @Type(value = DirectoryInternalBean.class, name = "internal"),
        @Type(value = DirectoryCrowdBean.class, name = "crowd"),
        @Type(value = DirectoryLdapBean.class, name = "ldap")
})
public abstract class AbstractDirectoryBean {

    @XmlElement
    private Long id;

    @XmlElement
    @NotNull
    @Size(min = 1)
    private String name;

    @XmlElement
    private String description;

    @XmlElement
    private boolean active;
}