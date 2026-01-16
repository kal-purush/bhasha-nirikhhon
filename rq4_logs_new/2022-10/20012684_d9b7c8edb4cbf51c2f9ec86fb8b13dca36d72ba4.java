package com.soffid.iam.federation.idp;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import edu.internet2.middleware.shibboleth.idp.session.IdPSessionFilter;
import es.caib.seycon.idp.ui.broker.SAMLSSOPostServlet;

public class SoffidIdPSessionFilter extends IdPSessionFilter {

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
			throws IOException, ServletException {
		HttpServletRequest req = (HttpServletRequest) request;
		if (req.getServletPath().equals(SAMLSSOPostServlet.URI))
			filterChain.doFilter(request, response);
		else
			super.doFilter(request, response, filterChain);
	}

}