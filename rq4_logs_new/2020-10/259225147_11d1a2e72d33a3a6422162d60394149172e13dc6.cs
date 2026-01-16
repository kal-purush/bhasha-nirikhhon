using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// 一定時間後にPortalを開ける
/// </summary>
public class Portal : MonoBehaviour
{
    [SerializeField] GoalIn goalIn;
    [SerializeField] Material portalMaterial;
    [SerializeField] float gageTime = 5f;
    float timer = kStartPortalGage;

    static readonly string kPlayer_tag = "Player";
    static readonly float kStartPortalGage = 0.6f;

    // Start is called before the first frame update
    void Start()
    {
        portalMaterial.SetFloat("Vector1_FABC1965", kStartPortalGage);
    }

    private void OnTriggerStay(Collider other)
    {
        if (!goalIn.isLock) return;
        if (other.gameObject.tag == kPlayer_tag)
        {
            if (timer >= 0)
            {
                timer -= (kStartPortalGage / gageTime) * Time.deltaTime;
                portalMaterial.SetFloat("Vector1_FABC1965", timer);
            }
            else
            {
                goalIn.isLock = false;
            }
        }
    }
}