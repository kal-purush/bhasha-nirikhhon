using System.Collections;
using UnityEngine;

namespace Player
{
	public class PlayerWallRun : MonoBehaviour
	{

		private bool _isWallR;
		private bool _isWallL;
		private RaycastHit _hitR;
		private RaycastHit _hitL;

		private Rigidbody _rb;
		private PlayerMovement _playerMovement;
		public float RunTime = 10f;
	
		// Use this for initialization
		void Start ()
		{
			_rb = GetComponent<Rigidbody>();
			_playerMovement = GetComponent<PlayerMovement>();
		}
	
		// Update is called once per frame
		void Update()
		{
			if (Input.GetKeyDown(KeyCode.E) && _playerMovement.GetJumpCount() <= 1)
			{
				if (Physics.Raycast(transform.position, transform.right, out _hitR, 1))
				{
					if (_hitR.transform.CompareTag("Wall"))
					{
						_isWallR = true;
						_isWallL = false;
						_playerMovement.ChangeJumpCount(-1);
						_rb.useGravity = false;
						StartCoroutine(AfterRun());
					}
				}
			}

			if (Physics.Raycast(transform.position, transform.right, out _hitR, 1))
			{
				if (_hitR.transform.CompareTag("Wall"))
				{
					_isWallL = true;
					_isWallR = false;
					_playerMovement.ChangeJumpCount(-1);
					_rb.useGravity = false;
					StartCoroutine(AfterRun());
				}
			}
		}

		IEnumerator AfterRun()
		{
			yield return new WaitForSeconds(RunTime);
			_isWallL = false;
			_isWallR = false;
			_rb.useGravity = true;
		}
	}
}
