using System;
using System.Linq;
using System.Net;
using System.Security.Principal;
using System.ServiceModel.Description;
using System.Web;
using Microsoft.IdentityModel.Claims;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;

namespace KpdApps.Common.MsCrm2015
{
	/// <summary>
	/// Implementation of OrganizationService.
	/// </summary>
	public sealed class CrmOrganizationService : IOrganizationService, IExtendedService, IDisposable
	{
		private OrganizationServiceProxy _organizationProxy;
		private bool _disposed;

		#region IExtendedService Members

		public ServiceProvider Provider { get; private set; }

		/// <summary>
		/// Proxy service initialization.
		/// </summary>
		/// <param name="provider"><see cref="ServiceProvider"/></param>
		public void Init(ServiceProvider provider)
		{
			try
			{
				Provider = provider;

				Uri uri = new Uri(provider.Settings.Service);
				IServiceConfiguration<IOrganizationService> orgConfig = ServiceConfigurationFactory.CreateConfiguration<IOrganizationService>(uri);
				ClientCredentials credentials = new ClientCredentials();
				if (orgConfig.AuthenticationType == AuthenticationProviderType.Federation)
				{
					// ADFS (IFD) auth
					credentials.UserName.UserName = provider.Settings.UserName;
					credentials.UserName.Password = provider.Settings.Password;
					_organizationProxy = new OrganizationServiceProxy(orgConfig, credentials);
					if (provider.Settings.UseDefaultCredentials)
					{
						_organizationProxy.CallerId = GetCurrentCrmUserId();
					}
				}
				else
				{
					// On-Premise, non-IFD auth
					if (provider.Settings.UseDefaultCredentials)
					{
						credentials.Windows.ClientCredential = CredentialCache.DefaultNetworkCredentials;
					}
					else
					{
						credentials.Windows.AllowedImpersonationLevel = TokenImpersonationLevel.Impersonation;
						credentials.Windows.ClientCredential = new NetworkCredential(provider.Settings.UserName, provider.Settings.Password, provider.Settings.Domain);
					}
					_organizationProxy = new OrganizationServiceProxy(uri, null, credentials, null);
				}
			}
			catch (Exception ex)
			{
				throw new ApplicationException("CrmOrganizationService initialization error;\n" + ex.Message);
			}
		}

		/// <summary>
		/// Get identifier of current crm user.
		/// </summary>
		/// <returns>SystemUser identifier.</returns>
		static public Guid GetCurrentCrmUserId()
		{
			const string crmUserClaimType = "http://schemas.microsoft.com/xrm/2011/Claims/CrmUser";

			Guid result = Guid.Empty;

			IClaimsIdentity callerIdentity = (IClaimsIdentity)(HttpContext.Current.User.Identity);
			Claim claim = callerIdentity.Claims.FirstOrDefault(c => c.ClaimType == crmUserClaimType);
			if (claim != null)
				result = new Guid(claim.Value);

			return result;
		}

		public void InitDependencies()
		{

		}

		#endregion

		/// <summary>
		/// Set organization proxy caller id.
		/// </summary>
		/// <param name="userId">SystemUser identifier.</param>
		public void SetCallerId(Guid userId)
		{
			_organizationProxy.CallerId = userId;
		}

		/// <summary>
		/// Get organization proxy caller id.
		/// </summary>
		/// <returns>SystemUser identifier.</returns>
		public Guid GetCallerId()
		{
			return _organizationProxy.CallerId;
		}

		/// <summary>
		/// Set current user as organization proxy caller.
		/// </summary>
		public void SetDefaultCallerId()
		{
			_organizationProxy.CallerId = GetCurrentCrmUserId();
		}

		#region IOrganizationService Members

		/// <summary>
		/// Create entity.
		/// </summary>
		/// <param name="entity">Entity.</param>
		/// <returns>Identifier of created entity.</returns>
		public Guid Create(Entity entity)
		{
			return _organizationProxy.Create(entity);
		}

		/// <summary>
		/// Delete entity.
		/// </summary>
		/// <param name="entityName">Entity logical name.</param>
		/// <param name="id">Entity identifier.</param>
		public void Delete(string entityName, Guid id)
		{
			_organizationProxy.Delete(entityName, id);
		}

		/// <summary>
		/// Associate entities.
		/// </summary>
		/// <param name="entityName">Entity logical name.</param>
		/// <param name="entityId">Entity identifier.</param>
		/// <param name="relationship">Relationship name.</param>
		/// <param name="relatedEntities">Collection of entities to associate.</param>
		public void Associate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
		{
			_organizationProxy.Associate(entityName, entityId, relationship, relatedEntities);
		}

		/// <summary>
		/// Disassociate.
		/// </summary>
		/// <param name="entityName">Entity logical name.</param>
		/// <param name="entityId">Entity identifier.</param>
		/// <param name="relationship">Relationship name.</param>
		/// <param name="relatedEntities">Collection of entities to associate.</param>
		public void Disassociate(string entityName, Guid entityId, Relationship relationship, EntityReferenceCollection relatedEntities)
		{
			_organizationProxy.Disassociate(entityName, entityId, relationship, relatedEntities);
		}

		/// <summary>
		/// Execute <see cref="OrganizationRequest"/>.
		/// </summary>
		/// <param name="request"><see cref="OrganizationRequest"/>.</param>
		/// <returns><see cref="OrganizationResponse"/></returns>
		public OrganizationResponse Execute(OrganizationRequest request)
		{
			return _organizationProxy.Execute(request);
		}

		/// <summary>
		/// Retrive entity by entity name and identifier.
		/// </summary>
		/// <param name="entityName">Entity name.</param>
		/// <param name="id">Entity identifier.</param>
		/// <param name="columnSet">Requested columns of entity.</param>
		/// <returns><see cref="Entity"/></returns>
		public Entity Retrieve(string entityName, Guid id, ColumnSet columnSet)
		{
			return _organizationProxy.Retrieve(entityName, id, columnSet);
		}

		/// <summary>
		/// Retrive <see cref="EntityCollection"/> by query.
		/// </summary>
		/// <param name="query"><see cref="QueryBase"/></param>
		/// <returns><see cref="EntityCollection"/></returns>
		public EntityCollection RetrieveMultiple(QueryBase query)
		{
			return _organizationProxy.RetrieveMultiple(query);
		}

		/// <summary>
		/// Update entity.
		/// </summary>
		/// <param name="entity"><see cref="Entity"/></param> 
		public void Update(Entity entity)
		{
			_organizationProxy.Update(entity);
		}

		#endregion

		#region IDisposable Members

		/// <summary>
		/// Implement IDisposable.
		/// </summary>
		public void Dispose()
		{
			if (_disposed)
				return;

			_organizationProxy?.Dispose();
			_disposed = true;
		}

		#endregion
	}
}