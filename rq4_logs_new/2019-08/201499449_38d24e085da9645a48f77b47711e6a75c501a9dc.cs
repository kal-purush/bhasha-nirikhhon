using UnityEngine;

public class EmployeeStressVisuals : MonoBehaviour
{
    [SerializeField] private Animator animator;
    [SerializeField] private SpriteRenderer renderer;
    [SerializeField] private Sprite happy;
    [SerializeField] private Sprite satisfied;
    [SerializeField] private Sprite average;
    [SerializeField] private Sprite sad;
    [SerializeField] private Sprite exhausted;
    [SerializeField] private Sprite scared;
    
    public void SetStressLevel(float stressLevel)
    {
        var sprite = GetSprite(stressLevel);
        renderer.sprite = sprite;
    }

    private Sprite GetSprite(float stressLevel)
    {
        if (stressLevel <= 0.15f)
        {
            return happy;
        }
        else if (stressLevel <= 0.3f)
        {
            return satisfied;
        }
        else if (stressLevel <= 0.45f)
        {
            return average;
        }
        else if (stressLevel <= 0.6f)
        {
            return sad;
        }
        else if (stressLevel <= 0.75f)
        {
            return exhausted;
        }
        else
        {
            animator.SetBool("Pulsate", true);
            return scared;
        }
    }
}