using DG.Tweening;
using UnityEngine;
using UnityEngine.UI;

namespace Flappy.UI
{
	/// <summary>
	/// トグル
	/// </summary>
	/// TODO: 必要に応じて値変更時のアクションを設定できるようにする
	public class CommonToggle : MonoBehaviour
	{
		/// <summary>
		/// Backgroundオブジェクト
		/// </summary>
		[SerializeField]
		private Image background;

		/// <summary>
		/// Handleオブジェクト
		/// </summary>
		[SerializeField]
		private Image handle;

		/// <summary>
		/// Checkオブジェクト
		/// </summary>
		[SerializeField]
		private Image check;

		/// <summary>
		/// Crossオブジェクト
		/// </summary>
		[SerializeField]
		private Image cross;

		/// <summary>
		/// チェックが入った状態で始まるか
		/// </summary>
		public bool IsAwakeWithTrue = false;

		/// <summary>
		/// チェック切り替えアニメの長さ (秒)
		/// </summary>
		const float aniamtionDuration = 0.1f;

		/// <summary>
		/// HandleオブジェクトのX座標
		/// </summary>
		/// <remarks>初期位置(Offの時の座標)を取得することで、中心からの距離が分かるので、Onの時はそれを反転する</remarks>
		private float handlePositionX;

		/// <summary>
		/// DOTweenのシーケンス保持用
		/// </summary>
		private Sequence dotweenSequence;

		/// <summary>
		/// トグル状態
		/// </summary>
		public bool Value
		{
			get
			{
				return this.value;
			}
			set
			{
				this.value = value;
				this.UpdateToggle();
			}
		}
		private bool value;

		/// <summary>
		/// Offの時の背景色
		/// </summary>
		private static readonly Color BgColorWhenDisabled = new Color(0.207843f, 0.239216f, 0.309804f);

		/// <summary>
		/// Onの時の背景色
		/// </summary>
		private static readonly Color BgColorWhenEnabled = new Color(0.827451f, 0.737255f, 0.556863f);

		/// <summary>
		/// Offの時のハンドル色
		/// </summary>
		private static readonly Color HandleColorWhenDisabled = new Color(0.925490f, 0.898039f, 0.847059f);

		/// <summary>
		/// Onの時のハンドル色
		/// </summary>
		private static readonly Color HandleColorWhenEnabled = new Color(1f, 0.972549f, 0.870588f);

		/// <summary>
		/// 初期化
		/// </summary>
		private void Start()
		{
			// 初期位置を取得
			this.handlePositionX = Mathf.Abs(this.handle.rectTransform.anchoredPosition.x);

			// 初期状態をセット
			this.value = IsAwakeWithTrue;

			// 表示を反映 (開始時なのでアニメーションさせない)
			this.UpdateToggle(0);
		}

		/// <summary>
		/// トグル状態を切り替える
		/// </summary>
		public void Switch()
		{
			this.Value = !this.Value;
		}

		/// <summary>
		/// 状態に応じて表示を更新する
		/// </summary>
		/// <param name="duration">切り替えアニメーションの長さ (秒)</param>
		private void UpdateToggle(float duration = CommonToggle.aniamtionDuration)
		{
			// Handle オブジェクト移動先のX座標
			var handleDestinationX = this.Value ? this.handlePositionX : -this.handlePositionX;

			// Handle オブジェクト変更先の色
			var handleDestinationColor = this.Value ? CommonToggle.HandleColorWhenEnabled : CommonToggle.HandleColorWhenDisabled;

			// Background オブジェクト変更先の色
			var backgroundDestinationColor = this.Value ? CommonToggle.BgColorWhenEnabled : CommonToggle.BgColorWhenDisabled;

			// Check オブジェクトの変更先のサイズ
			var checkDesticationScale = this.Value ? 1f : 0f;

			// Cross オブジェクトの変更先のサイズ
			var crossDesticationScale = this.Value ? 0f : 1f;

			// 既にアニメーションが行われていたら完了させる
			this.dotweenSequence?.Complete();

			// アニメーション処理
			this.dotweenSequence = DOTween.Sequence()
			.Append(this.handle.rectTransform.DOAnchorPosX(handleDestinationX, duration))
			.Join(this.handle.DOColor(handleDestinationColor, duration))
			.Join(this.background.DOColor(backgroundDestinationColor, duration))
			.Join(this.check.rectTransform.DOScale(checkDesticationScale, duration))
			.Join(this.cross.rectTransform.DOScale(crossDesticationScale, duration));
		}
	}
}