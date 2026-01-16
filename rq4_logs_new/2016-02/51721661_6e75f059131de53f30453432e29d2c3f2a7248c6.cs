using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace Halo.Unity.Common
{
	/// <summary>
	/// Provides direction services based on axis input that can optionally be relative to a transform.
	/// </summary>
	public interface IMovementDirectionService
	{
		/// <summary>
		/// Builds a normalized <see cref="Vector3"/> based on the input. Relative to the transform by default.
		/// </summary>
		/// <param name="input"><see cref="IMovementInput"/> service.</param>
		/// <param name="relativeToTransform">(Default: true) Indicates if this should be relative to the transform.</param>
		/// <returns>A normalized <see cref="Vector3"/> representing a movement direction based on axis input.</returns>
		Vector3 BuildDirection(IMovementInput input, bool relativeToTransform = true);

		/// <summary>
		/// Builds a normalized <see cref="Vector3"/> based on the input. Relative to the transform by default.
		/// </summary>
		/// <param name="vertical">Vertical axis input.</param>
		/// <param name="horizontal">Horizontal axis input.</param>
		/// <param name="relativeToTransform">(Default: true) Indicates if this should be relative to the transform.</param>
		/// <returns>A normalized <see cref="Vector3"/> representing a movement direction based on axis input.</returns>
		Vector3 BuildDirection(float vertical, float horizontal, bool relativeToTransform = true);
	}
}