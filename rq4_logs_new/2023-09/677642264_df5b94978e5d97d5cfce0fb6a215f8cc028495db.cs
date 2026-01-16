using Flappy.Manager;
using UnityEngine;

namespace Flappy
{
	/// <summary>
	/// Player ベースクラス
	/// </summary>
	public class PlayerBase : MonoBehaviour, IPlayer
	{
		/// <summary>
		/// Rigidbody2Dコンポーネント
		/// </summary>
		[SerializeField]
		protected new Rigidbody2D rigidbody2D;

		/// <summary>
		/// 死亡フラグ
		/// </summary>
		public bool IsDead => this.isDead;
		private bool isDead = false;

		/// <summary>
		/// PlayGameSceneオブジェクト
		/// </summary>
		/// TODO: こっちでシーンへの参照を持つのではなく、シーン側がPlayer側を監視したりするようにする
		[SerializeField]
		PlayGameScene playGameScene;

		/// <summary>
		/// 速度をセット
		/// </summary>
		/// <param name="speed">速度 (Rigidbody2Dのvelocityに渡す値)</param>
		protected void SetSpeed(Vector2 speed)
		{
			this.rigidbody2D.velocity = speed;
		}

		/// <summary>
		/// 指定した座標に移動
		/// </summary>
		/// <param name="position">移動先の座標</param>
		public void SetPosition(Vector2 position)
		{
			// this.transform.positionがプレイヤーに直通するフィールドなので、それに引数として位置情報を渡す
			this.transform.position = position;
		}

		/// <summary>
		/// 他のトリガコライダと接触した時のイベント
		/// </summary>
		/// <param name="collider">接触相手のコライダ</param>
		protected void OnTriggerEnter2D(Collider2D collider)
		{
			var otherLayer = collider.gameObject.layer;
			var otherLayerName = LayerMask.LayerToName(otherLayer);

			switch (otherLayerName)
			{
				case "Pillar":
					{
						this.OnTriggerPillar();
						break;
					}
				default:
					{
						// Nothing to do.
						break;
					}
			}
		}

		/// <summary>
		/// 柱に接触した時
		/// </summary>
		/// TOOD: Damage() みたいな関数名にして、ダメージを受けた時の処理をここに集約する
		///       今後、Pillar以外のギミックが増えることもあるし、シールドの処理が挟まったりもするため。
		protected void OnTriggerPillar()
		{
			// TODO: 死亡フラグで扱うよりPillar側の BoxCollider を無効にしたほうがよさそう
			if (isDead == true)
			{
				return;
			}

			AudioManager.Instance.PlaySE(Common.Constants.Assets.Audio.SE.boyon, 0.5f, 1.5f);

			// TODO: 必要に応じて定数化する
			float deathJumpForce = 600f;
			this.SetSpeed(Vector2.up * deathJumpForce);
			this.isDead = true;
			this.playGameScene.GameOver();
		}
	}
}