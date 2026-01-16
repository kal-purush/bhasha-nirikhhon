using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace KpdApps.Common.MsCrm2015.Helpers
{
	public static class CrmMetadataHelper
	{
		private static List<EntityMetadata> _entities = new List<EntityMetadata>();

		private static readonly object Locker = new object();

		public static EntityMetadata GetEntityMetadata(string entityName, IOrganizationService service)
		{
			lock (Locker)
			{
				if (_entities == null)
					_entities = new List<EntityMetadata>();
			}

			EntityMetadata metadata = _entities.FirstOrDefault(a => string.Compare(a.SchemaName, entityName, StringComparison.InvariantCultureIgnoreCase) == 0) ?? LoadEntityMetadata(entityName, service);

			return metadata;
		}

		private static EntityMetadata LoadEntityMetadata(string entityName, IOrganizationService service)
		{
			lock (Locker)
			{
				RetrieveEntityRequest request = new RetrieveEntityRequest
				{
					LogicalName = entityName,
					EntityFilters = EntityFilters.Entity | EntityFilters.Attributes
				};

				RetrieveEntityResponse metadata = (RetrieveEntityResponse)service.Execute(request);

				if (metadata.EntityMetadata == null)
					return null;

				_entities.Add(metadata.EntityMetadata);
				return metadata.EntityMetadata;
			}
		}
	}
}