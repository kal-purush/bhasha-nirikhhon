using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace Halo.Unity.Common
{
	public class LocalMotorServices : MonoBehaviour, IMovementParameters, IMovementInput,
		IMovementDirectionService
	{
		/// <summary>
		/// Current horizontal axis value.
		/// </summary>
		public float HorizontalAxis { get { return Input.GetAxis("Horizontal"); } }

		/// <summary>
		/// Current vertical axis value.
		/// </summary>
		public float VerticalAxis { get { return Input.GetAxis("Vertical"); } }

		/// <summary>
		/// Desired Jump intensity.
		/// </summary>
		[SerializeField]
		private float jumpIntensity;
		public float JumpIntensity { get { return jumpIntensity; } }

		/// <summary>
		/// Desired movement speed.
		/// </summary>
		[SerializeField]
		private float movementSpeed;
        public float MovementSpeed { get { return movementSpeed; } }

		/// <summary>
		/// Transform to use for relative direction computation.
		/// </summary>
		[SerializeField]
		private Transform RelativeTransform;

		[SerializeField]
		private KeyCode JumpKey;

		private bool isJumpedRequested;

		private void Awake()
		{
			RelativeTransform.ThrowIfNull();
		}

		//We have to poll input in Update for jumping.
		//Don't do it in fixed update; it will be inconsistent
		private void Update()
		{
			if (Input.GetKeyDown(JumpKey))
				isJumpedRequested = true;
        }

		/// <summary>
		/// Builds a normalized <see cref="Vector3"/> based on the input. Relative to the transform by default.
		/// </summary>
		/// <param name="input"><see cref="IMovementInput"/> service.</param>
		/// <param name="relativeToTransform">(Default: true) Indicates if this should be relative to the transform.</param>
		/// <returns>A normalized <see cref="Vector3"/> representing a movement direction based on axis input.</returns>
		public Vector3 BuildDirection(IMovementInput input, bool relativeToTransform = true)
		{
			return BuildDirection(input.VerticalAxis, input.HorizontalAxis, relativeToTransform);
		}

		/// <summary>
		/// Builds a normalized <see cref="Vector3"/> based on the input. Relative to the transform by default.
		/// </summary>
		/// <param name="vertical">Vertical axis input.</param>
		/// <param name="horizontal">Horizontal axis input.</param>
		/// <param name="relativeToTransform">(Default: true) Indicates if this should be relative to the transform.</param>
		/// <returns>A normalized <see cref="Vector3"/> representing a movement direction based on axis input.</returns>
		public Vector3 BuildDirection(float vertical, float horizontal, bool relativeToTransform = true)
		{
			if (relativeToTransform)
				return (transform.forward * vertical +
				   transform.right * horizontal).normalized;
			else
				return new Vector3(horizontal, 0f, vertical).normalized;
		}

		/// <summary>
		/// Indicates if a jump has been requested.
		/// If a jump has been requested then it consumes the internal jump request.
		/// </summary>
		/// <returns>True if a jump has been requested.</returns>
		public bool TryConsumeJumpRequest()
		{
			if (isJumpedRequested)
				return !(isJumpedRequested = false);
			else
				return isJumpedRequested;
        }
	}
}