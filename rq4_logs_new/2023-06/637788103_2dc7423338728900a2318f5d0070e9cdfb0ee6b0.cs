using DG.Tweening;
using UnityEngine;

namespace _Scripts.Powerups
{
	public class HeadSizeIncrease : Powerup
	{
		[SerializeField] private float _increaseAmount;

		private Vector3 _scale;
		
		protected override void Begin()
		{
			_scale = target.head.transform.localScale;
			Debug.Log($"{_scale}, scale");
			Debug.Log(_increaseAmount);
			
			target.head.transform.DOScale(_scale * _increaseAmount, 1f);
			Debug.Log($"Target scale: {_scale * _increaseAmount}");
		}

		protected override void End()
		{
			target.head.transform.DOScale(_scale, 1f);
		}

		protected override void ValuesToCopyToOther(Powerup pOther)
		{
			if (!(pOther is HeadSizeIncrease)) return;
			HeadSizeIncrease other = (HeadSizeIncrease)pOther;
			other._increaseAmount = _increaseAmount;
		}

		protected override void DisplayEffect()
		{
		}
	}
}