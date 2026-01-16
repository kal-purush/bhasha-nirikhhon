using Ralfred.Common.DataAccess.Context;
using Ralfred.Common.DataAccess.Entities;
using Ralfred.Common.Extensions;


namespace Ralfred.Common.DataAccess.Repositories
{
	public class GroupRepository : IGroupRepository
	{
		public GroupRepository(IStorageContext<Group> storageContext)
		{
			_storageContext = storageContext;
		}

		#region Implementation of IGroupRepository

		public bool Exists(string path)
		{
			var (groupName, groupPath) = path.DeconstructPath();
			var group = _storageContext.Find(g => g.Path.Equals(groupPath) && g.Name.Equals(groupName));

			return group != null;
		}

		public Group? FindByPath(string path)
		{
			var (groupName, groupPath) = path.DeconstructPath();

			var group = _storageContext.Find(g => g.Path.Equals(groupPath) && g.Name.Equals(groupName));

			return group;
		}

		#endregion

		private readonly IStorageContext<Group> _storageContext;
	}
}