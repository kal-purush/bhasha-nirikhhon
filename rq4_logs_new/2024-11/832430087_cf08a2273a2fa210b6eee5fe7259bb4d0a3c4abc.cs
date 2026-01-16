///<summary>
/// Author: Emily
///
/// Control the colour changing of the emissions on the player model
///
///</summary>

using UnityEngine;

namespace Millivolt
{
	public class PlayerEmissionController : MonoBehaviour
	{
		[Header("Emotion Colours")]
		[SerializeField] private Color m_defaultColour;
		[SerializeField] private Color m_happyColour;
		[SerializeField] private Color m_shockedColour;

		[Header("PlayerMaterials")]
		[SerializeField] private Material m_headMat;
		[SerializeField] private Material m_torsoMat;
		[SerializeField] private Material m_armsMat;
		[SerializeField] private Material m_legsMat;

		private void SetEmissionDefault()
		{

		}

        private void SetEmissionHappy()
        {

        }

        private void SetEmissionShocked()
        {

        }
    }
}