package de.aservo.confapi.commons.model;

import de.aservo.confapi.commons.constants.ConfAPI;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Bean for all user directories that are not supported by the plugin(s) yet.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@XmlRootElement(name = ConfAPI.DIRECTORY + '-' + ConfAPI.DIRECTORY_GENERIC)
public class DirectoryGenericBean extends AbstractDirectoryBean {

}