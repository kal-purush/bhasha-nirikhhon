using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using Unity.VisualScripting;
using UnityEngine.AddressableAssets;
using Flappy.Common;

namespace Flappy.Manager
{
	// TODO: 一度読み込んだアセットをキャッシュしておく仕組み
	// TODO: 初回再生するまでアセットが読み込まれないので、アセットを先読みする仕組み
	// TODO: 同時再生数の上限設定 (上限を超えるものはキュー方式で順番に再生する)
	// TODO: BGM用のAPI (再生、切り替え、停止など、フェード出来るとなおよい)
	public class AudioManager : SingletonMonoBehaviour<AudioManager>
	{
		/// <summary>
		/// AudioSources 初期数
		/// </summary>
		/// <remarks>足りない場合は自動で拡張する</remarks>
		[SerializeField]
		int defaultAudioInstances = 8;

		List<AudioSource> audioSources = new List<AudioSource>();

		void Start()
		{
			for (var i = 0; i < this.defaultAudioInstances; i++)
			{
				this.audioSources.Add(this.AddComponent<AudioSource>());
			}
		}

		public void PlaySE(Constants.Assets.Audio.SE se, float volume = 1f, float pitch = 1f)
		{
			var assetAddress = "Audio/SE/" + se.ToString();
			var asyncOperationHandle = Addressables.LoadAssetAsync<AudioClip>(assetAddress);
			asyncOperationHandle.Completed += (handle) =>
			{
				var audioClip = handle.Result;
				var audioSource = this.GetAvailableAudioSource();
				audioSource.clip = audioClip;
				audioSource.volume = volume;
				audioSource.pitch = pitch;
				audioSource.Play();
			};
		}

		AudioSource GetAvailableAudioSource()
		{
			var audioSource = this.audioSources.FirstOrDefault(_ => _.isPlaying == false);
			if (audioSource == null)
			{
				audioSource = this.AddComponent<AudioSource>();
				this.audioSources.Add(audioSource);
			}
			return audioSource;
		}
	}
}