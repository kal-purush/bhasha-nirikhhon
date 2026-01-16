using OriMod.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace OriMod.Core {
	public static class ModCore {
		public static bool CoreLoaded { get; private set; }

		internal static void OnGameStartup() {
			if (CoreLoaded) {
				return;
			}

			var th = new Thread(LoadCoreThread);
			th.Priority = ThreadPriority.AboveNormal;
			th.Start();

		}
		private static void LoadCoreThread() {
			try {
				AssetManager.Initialize();

				foreach (var dir in Directory.GetDirectories(AssetManager.ModdedContent)) {

					// Todo: check each mod for dll to load in
				}

				CoreLoaded = true;
			}
			catch (Exception ex) {
				TempLogger.Log(ex);

				UnityEngine.Application.Quit();
			}
		}
	}
}