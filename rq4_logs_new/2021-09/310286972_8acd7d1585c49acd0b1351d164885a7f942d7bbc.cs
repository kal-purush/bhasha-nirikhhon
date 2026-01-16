using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DamageEffectGenerator_R : MonoBehaviour
{
    [SerializeField] private GameObject[] damageEffect;
    [SerializeField] private EvolutionChicken_R scrEvo;
    [SerializeField] private Parameters_R scrParam;

    private int HP;

    void Start()
    {
        HP = scrParam.hp;
    }

    // Update is called once per frame
    void Update()
    {
        // HPが減少していたらEffectを生成
        if(scrParam.hp < HP)
        {
            GameObject obj = Instantiate(damageEffect[scrEvo.EvolutionNum], transform);
            obj.transform.position = transform.position + (transform.localScale.y / 2) * Vector3.up;
            Destroy(obj, 1.0f);
        }
        HP = scrParam.hp;
    }
}