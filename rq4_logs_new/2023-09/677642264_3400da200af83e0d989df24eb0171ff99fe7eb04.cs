using System.Collections;
using DG.Tweening;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.UI;

namespace Flappy.UI
{
	/// <summary>
	/// フルスクリーンロード画面 (プログレスバーなし)
	/// </summary>
	public class Loading3 : MonoBehaviour
	{
		/// <summary>
		/// CanvasオブジェクトのCanvasGroupコンポーネント
		/// </summary>
		[SerializeField]
		CanvasGroup canvasGroup;

		/// <summary>
		/// Animator コンポーネント
		/// </summary>
		[SerializeField]
		Animator animator;

		/// <summary>
		/// フェードイン・アウトのデフォルト時間 (秒)
		/// </summary>
		const float defaultFadeTime = 0.4f;

		/// <summary>
		/// 最低表示時間 (秒)
		/// </summary>
		const float minDisplayTime = 1.5f;

		/// <summary>
		/// 経過時間
		/// </summary>
		float elapsedTime = 0f;

		/// <summary>
		/// ロード開始時(フェードイン完了後)に実行するアクション
		/// </summary>
		UnityAction onBeginLoad;

		/// <summary>
		/// ロード完了時(フェードイン開始前)に実行するアクション
		/// </summary>
		UnityAction onCompleteLoad;

		/// <summary>
		/// 初期化
		/// </summary>
		private void Start()
		{
			// アニメーションをランダムな位置から再生する
			animator.Play(animator.GetCurrentAnimatorStateInfo(0).shortNameHash, 0, Random.Range(0f, 1f));
		}

		/// <summary>
		/// 更新
		/// </summary>
		private void Update()
		{
			this.elapsedTime += Time.deltaTime;
		}

		/// <summary>
		/// ロード画面を表示する
		/// </summary>
		/// <param name="fadeTime">フェードイン時間</param>
		/// <param name="onBeginLoad">ロード開始時(フェードイン完了後)に実行するアクション</param>
		/// <param name="onCompleteLoad">ロード完了時(フェードイン開始前)に実行するアクション</param>
		public void Show(float fadeTime = Loading3.defaultFadeTime, UnityAction onBeginLoad = null, UnityAction onCompleteLoad = null)
		{
			this.onBeginLoad = onBeginLoad;
			this.onCompleteLoad = onCompleteLoad;

			this.gameObject.SetActive(true);
			this.canvasGroup.alpha = 0f;
			this.canvasGroup.DOFade(1f, fadeTime).OnComplete(() =>
			{
				this.onBeginLoad?.Invoke();
			});
		}

		/// <summary>
		/// ロード画面を非表示にする
		/// </summary>
		/// <param name="fadeTime">フェードアウト時間</param>
		public void Hide(float fadeTime = Loading3.defaultFadeTime)
		{
			// 最低表示時間が経過するまで待機
			this.StartCoroutine(this.WaitForMinDisplayTime(() =>
			{
				this.onCompleteLoad?.Invoke();

				this.canvasGroup.alpha = 1f;
				this.canvasGroup.DOFade(0f, fadeTime).OnComplete(() =>
				{
					GameObject.Destroy(this.gameObject);
				});
			}));
		}

		/// <summary>
		/// 最低表示時間が経過するまで待機する
		/// </summary>
		/// <param name="completed">待機後に実行するアクション</param>
		private IEnumerator WaitForMinDisplayTime(UnityAction completed)
		{
			while (this.elapsedTime < Loading3.minDisplayTime)
			{
				yield return null;
			}
			completed?.Invoke();
		}
	}
}