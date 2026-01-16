using Suhdo.Ultils;
using UnityEngine;

namespace Suhdo.FX
{
	public class AfterImageSprite : PoolableMonoBehaviour
	{
		[SerializeField] private float activeTime = 0.1f;
		[SerializeField] private float alphaSet = 0.8f;
		[SerializeField] private float alphaDecay = 0.85f;
		private float timeActivated;
		private float alpha;

		private Transform _transform;

		private SpriteRenderer _localSR;
		private SpriteRenderer _tranformSR;

		private Color color;
		
		public override void OnObjectPoolReturn()
		{
			
		}

		public void StartAfterImage(Transform data)
		{
			_transform = (Transform) data;
			_localSR ??= GetComponent<SpriteRenderer>();
			_tranformSR = _transform.GetComponent<SpriteRenderer>();

			alpha = alphaSet;
			_localSR.sprite = _tranformSR.sprite;
			transform.position = _transform.position;
			transform.rotation = _transform.rotation;
			timeActivated = Time.time;
		}

		private void Update()
		{
			if (_transform == null) return;
			
			alpha -= alphaDecay * Time.deltaTime;
			color = new Color(1f, 1f, 2f, alpha);
			_localSR.color = color;

			if (Time.time >= (timeActivated + activeTime))
			{
				//Add back to pool.
				Release();
			}
		}
	}
}