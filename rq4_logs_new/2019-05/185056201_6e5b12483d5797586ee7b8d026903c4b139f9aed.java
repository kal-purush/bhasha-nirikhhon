package com.xywei.authorization;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.Subject;
import org.junit.Test;

import com.xywei.realm.MyCustomRealm_Authorization;

public class Authorization {

	@Test
	public void testAuthorization() {

		MyCustomRealm_Authorization realm_Authorization = new MyCustomRealm_Authorization();
		DefaultSecurityManager securityManager = new DefaultSecurityManager();
		securityManager.setRealm(realm_Authorization);
		SecurityUtils.setSecurityManager(securityManager);

		String username = "admin";
		String password = "admin";

		UsernamePasswordToken token = new UsernamePasswordToken(username, password);

		Subject subject = SecurityUtils.getSubject();

		try {
			subject.login(token);
			System.out.println(subject.isAuthenticated());

			System.out.println("hash admin:===" + subject.hasRole("admin1"));
			System.out.println("hash user:===" + subject.hasRole("user1"));
			System.out.println("is permitted user:select:===" + subject.isPermitted("user:select1"));
			System.out.println("is permitted admin:select:===" + subject.isPermitted("admin:select1"));
		} catch (AuthenticationException e) {
			e.printStackTrace();
		}

	}

}