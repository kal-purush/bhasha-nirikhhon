using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class healthBar : MonoBehaviour
{
    public Health hp;
    public Image current_hp;
    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        updateHealth();
    }

    public void updateHealth()
    {
        float damage_ratio = hp.RemainingHP / hp.MaxHP;
        current_hp.rectTransform.localScale = new Vector3(damage_ratio, 1, 1);
    }
}