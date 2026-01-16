package org.cru.migration.service;

import com.google.common.collect.Sets;
import org.ccci.idm.ldap.Ldap;
import org.cru.migration.domain.RelayUser;
import org.cru.migration.service.execution.ExecuteAction;
import org.cru.migration.service.execution.ExecutionService;
import org.cru.migration.support.FileHelper;
import org.cru.migration.support.MigrationProperties;
import org.cru.migration.support.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NamingException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class AuthenticationService
{
	private MigrationProperties properties;

    private static Logger logger = LoggerFactory.getLogger(AuthenticationService.class);

	private Set<String> successAuthentication = Sets.newConcurrentHashSet();
	private Set<String> failedAuthentication = Sets.newConcurrentHashSet();

    public AuthenticationService()
	{
		properties = new MigrationProperties();
	}

    public void authenticate(Set<RelayUser> relayUsers) throws NamingException
	{
		ExecutionService executionService = new ExecutionService();

		AuthenticateData authenticateData = new AuthenticateData(relayUsers);

		executionService.execute(new Authenticate(), authenticateData, 200);

		Output.logMessage(successAuthentication,
				FileHelper.getFileToWrite(properties.getNonNullProperty("successAuthentication")));
		Output.logMessage(failedAuthentication,
				FileHelper.getFileToWrite(properties.getNonNullProperty("failedAuthentication")));
	}

	private class AuthenticateData
	{
		private Set<RelayUser> relayUsers;

		public AuthenticateData(Set<RelayUser> relayUsers)
		{
			this.relayUsers = relayUsers;
		}
	}

	public class Authenticate implements ExecuteAction
	{
		@Override
		public void execute(ExecutorService executorService, Object object)
		{
			AuthenticateData authenticateData = (AuthenticateData)object;

			for (RelayUser relayUser : authenticateData.relayUsers)
			{
				executorService.execute(new AuthenticationThread(relayUser));
			}
		}
	}

	private class AuthenticationThread implements Runnable
	{
		private RelayUser relayUser;

		public AuthenticationThread(RelayUser relayUser)
		{
			this.relayUser = relayUser;
		}

		@Override
		public void run()
		{
			try
			{
				String dn = "cn=" + relayUser.getUsername() + "," + properties.getNonNullProperty("theKeyMergeUserRootDn");

				new Ldap(properties.getNonNullProperty("theKeyLdapHost"), dn, relayUser.getPassword());

				successAuthentication.add("" + relayUser.getUsername());
			}
			catch(Exception e)
			{
				failedAuthentication.add("" + relayUser.getUsername());
			}
		}
	}
}