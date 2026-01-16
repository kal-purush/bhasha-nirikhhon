using System;
using System.Collections.Generic;

namespace ImageManager.Settings
{
	[Serializable]
	class Settings
	{
		public string SubfolderName { get; }
		public string Key { get; }
		public bool? IsCopyFileMode { get; }

		public Settings(string subfolderName, string bindedKey, bool? isCopyFileMode)
		{
			SubfolderName = subfolderName;
			Key = bindedKey;
			IsCopyFileMode = isCopyFileMode;
		}
	}
}
