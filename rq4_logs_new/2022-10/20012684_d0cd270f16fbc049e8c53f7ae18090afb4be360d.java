package es.caib.seycon.idp.ui;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Cookie;
import org.apache.commons.logging.LogFactory;

import com.soffid.iam.addons.federation.common.FederationMember;
import com.soffid.iam.addons.federation.common.IdentityProviderType;
import com.soffid.iam.addons.federation.remote.RemoteServiceLocator;

import es.caib.seycon.idp.config.IdpConfig;
import es.caib.seycon.idp.server.AuthenticationContext;
import es.caib.seycon.idp.shibext.LogRecorder;
import es.caib.seycon.idp.ui.broker.SAMLSSORequest;
import es.caib.seycon.idp.ui.oauth.OauthRequestAction;
import es.caib.seycon.ng.exception.InternalErrorException;

public class ChangeUserAction extends HttpServlet {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	LogRecorder logRecorder = LogRecorder.getInstance();

    public static final String URI = "/changeUserName"; //$NON-NLS-1$

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        String error = null;
       	AuthenticationContext ctx = AuthenticationContext.fromRequest(req);
       	ctx.setUser(null);
       	try {
			ctx.updateAllowedAuthenticationMethods();
		} catch (Exception e) {
           	error = "Cannot find suitable authentication methods";
           	LogFactory.getLog(getClass()).warn("Cannot find suitable authentication methods", e);
		}
       	error = null;
        req.setAttribute("ERROR", error); //$NON-NLS-1$
        RequestDispatcher dispatcher = req.getRequestDispatcher(UserPasswordFormServlet.URI);
        dispatcher.forward(req, resp);
    }

    
}
