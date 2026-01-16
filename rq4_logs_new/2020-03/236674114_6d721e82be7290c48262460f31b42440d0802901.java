package gt.edu.usac.cunoc.ingenieria.eps.configuration;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import javax.security.enterprise.identitystore.DatabaseIdentityStoreDefinition;
import javax.security.enterprise.identitystore.Pbkdf2PasswordHash;

@DatabaseIdentityStoreDefinition(
        dataSourceLookup = SecurityConstants.DATASOURCE_LOOKUP,
        callerQuery = SecurityConstants.CALLER_QUERY,
        groupsQuery = SecurityConstants.GROUPS_QUERY,
        hashAlgorithm = Pbkdf2PasswordHash.class,
        hashAlgorithmParameters = {
            "${applicationConfig.dyna}"
        },
        priority = 10
)
@ApplicationScoped
@Named
public class ApplicationConfig {

    public String[] getDyna() {
        return new String[]{
            SecurityConstants.PBKDF_ITERATIONS,
            SecurityConstants.PBKDF_ALGORITHM,
            SecurityConstants.PBKDF_SALT_SIZE
        };
    }
}