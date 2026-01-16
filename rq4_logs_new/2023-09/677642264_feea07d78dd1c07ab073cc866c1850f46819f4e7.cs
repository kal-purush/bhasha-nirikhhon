using System;
using UnityEngine;
using Flappy.Common;
using System.Collections.Generic;

namespace Flappy.Utility
{
	/// <summary>
	/// Addressablesからアセットを取得するためのユーティリティ
	/// </summary>
	public class AssetAddressUtility
	{
		/// <summary>
		/// アセットタイプごとのディレクトリ定義
		/// </summary>
		private static readonly Dictionary<Type, string> directoryMap = new()
		{
			{typeof(Constants.Assets.Sprite.ButtonIcon),            "Sprite/ButtonIcon/"},
			{typeof(Constants.Assets.Audio.SE),                     "Audio/SE/"},
		};

		/// <summary>
		/// Addressablesのアセットアドレスを取得
		/// </summary>
		/// <param name="asset">アセットの種類<para>Constants.Assets.* 配下のenum定数を渡す</para></param>
		public static string GetAssetAddress(Enum asset)
		{
			Type assetType = asset.GetType();

			string directory;
			if (AssetAddressUtility.directoryMap.TryGetValue(assetType, out directory) == false)
			{
				Debug.LogError($"未知のアセットタイプが指定されました。アセットタイプ: {assetType}");
				return null;
			}

			return directory + asset.ToString();
		}
	}
}