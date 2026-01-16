using UnityEngine;
using System.Collections;

public class LerpLightObject : MonoBehaviour
{
		public float ZStart = -10.0f;
		public float ZEnd = -30.0f;
		public float TotalLifetime = 180.0f;
		private float Timer = 0.1f;

		// Use this for initialization
		void Start ()
		{
		
		}
	
		// Update is called once per frame
		void Update ()
		{
				Vector3 pos = this.transform.position;
				Vector3 start = new Vector3 (pos.x, pos.y, ZStart);
				Vector3 end = new Vector3 (pos.x, pos.y, ZEnd);
				pos = Vector3.Lerp (start, end, (Timer / TotalLifetime));
				this.transform.position = pos;
				if (Timer < TotalLifetime)
						Timer += Time.deltaTime;
		}
}