package pl.edu.icm.oxides.authn;

import eu.emi.security.authn.x509.X509Credential;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.stereotype.Component;
import pl.edu.icm.oxides.user.OxidesPortalGridSession;
import pl.edu.icm.unicore.spring.security.GridIdentityProvider;
import pl.edu.icm.unity.spring.UnityAutoConfiguration;
import pl.edu.icm.unity.spring.slo.UnitySingleLogoutContext;

@Component
@AutoConfigureBefore(UnityAutoConfiguration.class)
public class OxidesSingleLogoutContext implements UnitySingleLogoutContext {
    private final GridIdentityProvider gridIdentityProvider;
    private OxidesPortalGridSession oxidesPortalGridSession;

    @Autowired
    public OxidesSingleLogoutContext(GridIdentityProvider gridIdentityProvider,
                                     OxidesPortalGridSession oxidesPortalGridSession) {
        this.gridIdentityProvider = gridIdentityProvider;
        this.oxidesPortalGridSession = oxidesPortalGridSession;
    }

    @Override
    public String getPrincipalCommonName() {
        return oxidesPortalGridSession.getDistinguishedName();
    }

    @Override
    public String getSessionIndex() {
        return oxidesPortalGridSession.getSessionIndex();
    }

    @Override
    public X509Credential getGridCredential() {
        return gridIdentityProvider.getGridCredential();
    }
}