using System;
using System.Collections.Generic;

using Ralfred.Common.DataAccess.Entities;
using Ralfred.Common.DataAccess.Repositories;
using Ralfred.Common.Helpers;
using Ralfred.Common.Types;


namespace Ralfred.Common.Providers
{
	public class SecretsProvider
	{
		public IEnumerable<Secret> GetSecrets(string path, string[] secrets)
		{
			var pathType = _pathResolver.Resolve(path);

			switch (pathType)
			{
				case PathType.None:
					// TODO: change to custom exception
					throw new Exception("Path not found");
				case PathType.Secret:
				{
					var (name, groupPath) = _pathResolver.DeconstructPath(path);
					var (groupName, folderPath) = _pathResolver.DeconstructPath(groupPath);
					var secret = _secretsRepository.GetGroupSecrets(groupName, folderPath);

					return new Secret[] { secret };
				}
				case PathType.Group:
				{
					var (groupName, folderPath) = _pathResolver.DeconstructPath(path);

					return _secretsRepository.GetGroupSecrets(groupName, folderPath);
				}
				default:
					throw new Exception("WTF");
			}
		}

		private readonly IPathResolver _pathResolver;
		private readonly ISecretsRepository _secretsRepository;
	}
}