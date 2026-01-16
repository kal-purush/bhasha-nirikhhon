using dreamlet.DataAccessLayer.Entities.Base;
using dreamlet.DataAccessLayer.MongoDbContext;
using DryIocAttributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dreamlet.DataAccessLayer.Repository
{
	[Export]
	public class RepositoryFactory
	{
		private static readonly object _locker = new object();
		private Dictionary<string, object> _repositories;

		[Import]
		public IMongoContext MongoDatabaseContext { get; set; }

		public RepositoryFactory(IMongoContext context = null)
		{
			if (context != null)
				MongoDatabaseContext = context;
		}

		public IRepository<TDocument> Get<TDocument>(IMongoContext mongoDatabaseContext = null) where TDocument : IBaseMongoEntity
		{
			lock (_locker)
			{
				if (_repositories == null)
					_repositories = new Dictionary<string, object>();

				var type = typeof(TDocument).Name;

				if (!_repositories.ContainsKey(type))
					_repositories.Add(type, new Repository<TDocument>(MongoDatabaseContext ?? mongoDatabaseContext));

				return (IRepository<TDocument>)_repositories[type];
			}
		}

	}
}