package com.chlol.gstone.shiro;

import java.util.Collection;
import java.util.List;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import com.chlol.gstone.shiro.impl.DefaultUserProfileManager;

public class GStoneAuthorizingRealm extends AuthorizingRealm {

	private AuthenticationToken token;	

	public GStoneAuthorizingRealm() {
		super();
		SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
	}

	@Autowired
	private ApplicationContext appContext;
	public IUserProfileManager getUserProfileManager() {
		try {
			IUserProfileManager upm = (IUserProfileManager) appContext.getBean(IUserProfileManager.UserProfileManager_Name);
			return upm;
		} catch (Exception e) {
			return new DefaultUserProfileManager();
		}
		
	}
	@Override
	protected AuthorizationInfo doGetAuthorizationInfo(
			PrincipalCollection principals) {
		if (token == null) {
			return null;
		}
		UserProfileToken userProfileToken = (UserProfileToken) token;
		SimpleAuthorizationInfo authorizationInfo = new SimpleAuthorizationInfo();

		Collection<String> roles = userProfileToken.getUserProfile().getRoles()
				.keySet();
		authorizationInfo.addRoles(roles);

		List<String> stringPermissions = userProfileToken.getUserProfile()
				.getPermissions();
		authorizationInfo.addStringPermissions(stringPermissions);
		return authorizationInfo;
	}

	@Override
	protected AuthenticationInfo doGetAuthenticationInfo(
			AuthenticationToken token) {
		UserProfileToken userProfileToken = (UserProfileToken) token;
		String username = userProfileToken.getUserProfile().getLoginName();
		String password = userProfileToken.getUserProfile().getPassword();
		this.token = token;
		if (password == null) {
			throw new AuthenticationException("");
		}
		UserProfile userProfile = getUserProfileManager().checkUser(username,
				password);
		userProfileToken.setUserProfile(userProfile);
		userProfile.setPassword(password);
		if (userProfile == null || userProfile.getLoginMessage() != null) {
			throw new AuthenticationException("");
		}
		
		SimpleAuthenticationInfo simpleInfo = new SimpleAuthenticationInfo(
				username, password, getName());
		clearCache(simpleInfo.getPrincipals());
		return simpleInfo;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getAuthenticationTokenClass() {
		return UserProfileToken.class;
	}
}