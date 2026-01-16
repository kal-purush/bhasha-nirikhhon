using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using System;
namespace UnityStandardAssets.Vehicles.Car{
//bastek 16.05.2015, 17.05.2015
	[RequireComponent(typeof(CarController))]

	public class GameController : MonoBehaviour 
	{
		//components
		public int CheckpointCounter;
		public Text CheckpointGuiText;
		public Text CountingText;
		public Text TimeText;
		public GameObject LastCheckPointCamera;
		public Animator CheckpointAnimatorController;
		public Transform CameraPosition;

		[SerializeField]
		private int _pointsToWin;
		private CarController _carController;
		private bool _coroutinState = false;
		private float _timeFloat;


	
		void Start()
		{
			_pointsToWin = 9;
			_carController = GetComponent<CarController> ();
		}

		void Update()
		{
			if (!_coroutinState) 
			{
				if (Vector3.Distance (CameraPosition.position, gameObject.transform.position) < 2.6f) 
				{

					StartCoroutine(StartCounting());
				}

			}
			_timeFloat = Mathf.Floor(Time.time) - 4f;
			TimeText.text = _timeFloat.ToString();
		}

		void OnTriggerEnter (Collider other)
		{
			other.enabled = !other.enabled;
			CheckPointAnimation ();
			CheckpointCounter++;

			if(CheckpointCounter == _pointsToWin)
			{
				LastCheckPointCamera.SetActive(true);
				_carController.enabled = !_carController.enabled;

			}

		}

		void CheckPointAnimation()
		{
			CheckpointAnimatorController.SetTrigger("Switch");
			CheckpointAnimatorController.SetTrigger ("FadeOut");
		}

		IEnumerator StartCounting()
		{
			_coroutinState = true;
				for (int i = 0; i < 4; i++) 
				{
					CountingText.text = i.ToString();
					yield return new WaitForSeconds(1f);

					
				}
				CountingText.text  = "GO!";
				CountingText.enabled = !CountingText.enabled;
			_carController.enabled = !_carController.enabled;


		}

	}
}