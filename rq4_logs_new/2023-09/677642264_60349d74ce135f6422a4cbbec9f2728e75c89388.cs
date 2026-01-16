using UnityEngine;
using UnityEngine.UI;
using TMPro;
using Flappy.Manager;

namespace Flappy.PlayGame
{
	/// <summary>
	/// スコアゲージ
	/// </summary>
	public class ScoreGauge : MonoBehaviour
	{
		/// <summary>
		/// 現在スコアテキスト
		/// </summary>
		[SerializeField]
		private TextMeshProUGUI currentScoreText;

		/// <summary>
		/// 最高スコアテキスト
		/// </summary>
		[SerializeField]
		private TextMeshProUGUI bestScoreText;

		/// <summary>
		/// CurrentScoreMaskのImageコンポーネント
		/// </summary>
		[SerializeField]
		private Image currentScoreMask;

		/// <summary>
		/// BestScoreMaskのImageコンポーネント
		/// </summary>
		[SerializeField]
		private Image bestScoreMask;

		/// <summary>
		/// ゲージの横幅
		/// </summary>
		private float gaugeWidth;

		/// <summary>
		/// 初期化
		/// </summary>
		private void Start()
		{
			// 最高スコアが0の時の表示

			// ゲージの横幅を取得
			this.gaugeWidth = (this.transform as RectTransform).sizeDelta.x;

			// 最高スコア表示は初期化時にセット
			this.SetBestScoreText(GameManager.Instance.BestScore);
		}

		/// <summary>
		/// 現在スコアをゲージに反映する
		/// </summary>
		/// <param name="currentScore">現在スコア</param>
		public void SetCurrentScore(float currentScore)
		{
			// スコアを少数第一位まで丸める
			currentScore = GameManager.Instance.RoundScore(currentScore);

			// ゲージの塗りつぶし状態を更新
			this.UpdateGaugeFill(currentScore);

			// 最高スコアアイコンの位置を更新
			this.UpdateBestIconPosition(currentScore);

			// 現在スコアテキストをセット
			this.SetCurrentScoreText(currentScore);
		}

		/// <summary>
		/// ゲージの塗りつぶし状態を更新
		/// </summary>
		/// <param name="currentScore">現在スコア (丸めてから渡す)</param>
		private void UpdateGaugeFill(float currentScore)
		{
			// 最高スコアがない時は何もしない
			if (GameManager.Instance.IsRecordedBestScore == true)
			{
				return;
			}

			// 最高スコアに対する現在スコアの割合
			var currentToBestRatio = currentScore / GameManager.Instance.BestScore;

			// 最高スコアに到達するまでのゲージ塗りつぶし
			this.currentScoreMask.fillAmount = Mathf.Clamp01(currentToBestRatio);

			// 現在スコアが最高スコアをどれくらい超えているか
			if (currentScore > GameManager.Instance.BestScore)
			{
				// 超えた割合の分だけ右から塗りつぶされる
				this.bestScoreMask.fillAmount = 1f - (1f / currentToBestRatio);
			}
		}

		/// <summary>
		/// 最高スコアアイコンの位置を更新
		/// </summary>
		/// <param name="currentScore">現在スコア (丸めてから渡す)</param>
		private void UpdateBestIconPosition(float currentScore)
		{

		}

		/// <summary>
		/// 現在スコアテキストをセット
		/// </summary>
		/// <param name="currentScore">現在スコア</param>
		private void SetCurrentScoreText(float currentScore)
		{
			this.currentScoreText.text = GameManager.Instance.ScoreToText(currentScore);
		}

		/// <summary>
		/// 最高スコアテキストをセット
		/// </summary>
		/// <param name="bestScore">最高スコア</param>
		private void SetBestScoreText(float bestScore)
		{
			this.bestScoreText.text = GameManager.Instance.ScoreToText(bestScore);
		}
	}
}