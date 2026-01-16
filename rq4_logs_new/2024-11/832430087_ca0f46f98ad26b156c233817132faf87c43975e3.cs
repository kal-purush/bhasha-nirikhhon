///<summary>
/// Author: Halen
///
/// Shows the player if they are on the ceiling or floor.
///
///</summary>

using UnityEngine;
using UnityEngine.UI;
using Pixelplacement;
using System.Collections;

namespace Millivolt
{
	public class GravityIndicatorUI : MonoBehaviour
	{
		[SerializeField] private Image m_downArrow;
		[SerializeField] private Image m_upArrow;

		[Header("Colours")]
		[SerializeField] private Color m_disabledColour = new(0, 0, 0, 1);
		[SerializeField] private Color m_downColour = new(0, 0, 0, 1);
		[SerializeField] private Color m_upColour = new(0, 0, 0, 1);

		[Header("Enable Transition")]
		[SerializeField, Min(0)] private float m_alphaDuration;
		[SerializeField, Min(0)] private float m_alphaDelay;

        [Header("Colour Transition")]
        [SerializeField, Min(0)] private float m_colourDuration;
        [SerializeField, Min(0)] private float m_colourDelay;

        [Header("Arrow Grow and Shrink")]
        [SerializeField, Min(1)] private Vector3 m_growScale;
        [SerializeField, Min(0)] private Vector3 m_shrinkScale;
        [SerializeField, Min(0)] private float m_growDuration;
        [SerializeField, Min(0)] private float m_shrinkDuration;
        [SerializeField, Min(0)] private float m_growAndShrinkDelay;

		public void ChangeToDown()
		{
            StopAllCoroutines();
            Tween.Stop(GetInstanceID());

            // change colours
            StartCoroutine(ChangeColourOverTime(m_downArrow, m_downArrow.color, m_downColour));
			StartCoroutine(ChangeColourOverTime(m_upArrow, m_upArrow.color, m_disabledColour));

            // change scale
            Tween.LocalScale(m_downArrow.rectTransform, m_growScale, m_growDuration, m_growAndShrinkDelay, Tween.EaseIn);
            Tween.LocalScale(m_upArrow.rectTransform, m_shrinkScale, m_shrinkDuration, m_growAndShrinkDelay, Tween.EaseOut);
            //StartCoroutine(GrowAndShrink(m_downArrow.rectTransform, Vector3.one, m_growScale));
		}

        public void ChangeToUp()
		{
            StopAllCoroutines();
            Tween.Stop(GetInstanceID());

            // change colours
            StartCoroutine(ChangeColourOverTime(m_downArrow, m_downArrow.color, m_disabledColour));
            StartCoroutine(ChangeColourOverTime(m_upArrow, m_upArrow.color, m_upColour));

            // change scale
            Tween.LocalScale(m_downArrow.rectTransform, m_shrinkScale, m_shrinkDuration, m_growAndShrinkDelay, Tween.EaseOut);
            Tween.LocalScale(m_upArrow.rectTransform, m_growScale, m_growDuration, m_growAndShrinkDelay, Tween.EaseIn);
            //StartCoroutine(GrowAndShrink(m_upArrow.rectTransform, Vector3.one, m_growScale));
        }

        public void SetToDown()
        {
            StopAllCoroutines();
            Tween.Stop(GetInstanceID());

            // set colours
            m_downArrow.color = m_downColour;
            m_upArrow.color = m_disabledColour;

            // set scale
            m_downArrow.rectTransform.localScale = m_growScale;
            m_upArrow.rectTransform.localScale = m_shrinkScale;
        }

        public void SetToUp()
        {
            StopAllCoroutines();
            Tween.Stop(GetInstanceID());

            // set colours
            m_downArrow.color = m_disabledColour;
            m_upArrow.color = m_upColour;

            // set scale
            m_downArrow.rectTransform.localScale = m_shrinkScale;
            m_downArrow.rectTransform.localScale = m_growScale;
        }

        private void OnEnable()
        {
			CanvasGroup group = GetComponent<CanvasGroup>();
			group.alpha = 0;

            Tween.Stop(GetInstanceID());
            Tween.CanvasGroupAlpha(group, 1, m_alphaDuration, m_alphaDelay, Tween.EaseIn);

            SetToDown();
        }

        private IEnumerator ChangeColourOverTime(Image arrow, Color startColour, Color endColour)
		{
            float timer = 0;

            Vector3 start = new(startColour.r, startColour.g, startColour.b);
            Vector3 end = new(endColour.r, endColour.g, endColour.b);

            while (timer < m_colourDuration)
            {
                timer += Time.deltaTime;

                float t = timer / m_colourDuration;

                t = -(Mathf.Cos(Mathf.PI * t) - 1f) / 2f;

                // get vector value
                Vector3 currentColour = Vector3.Slerp(start, end, t);

                // set colour from vector values (alpha is always 1)
                arrow.color = new(currentColour.x, currentColour.y, currentColour.z, 1);

                yield return null;
            }
        }

        private IEnumerator GrowAndShrink(RectTransform t, Vector3 startScale, Vector3 endScale)
        {
            Tween.LocalScale(t, endScale, m_growDuration, m_growAndShrinkDelay);
            yield return new WaitForSeconds(m_growDuration + m_growAndShrinkDelay);
            Tween.LocalScale(t, startScale, m_shrinkDuration, 0);
        }
    }
}