using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StopCivil_Y : Civil_Y
{
    [SerializeField] private string[] animStrArray;
    public float animCycle;
    // Start is called before the first frame update
    private void Start()
    {
        rb = GetComponent<Rigidbody>();
        player = GameObject.Find("Player");
        criAtomSource = GetComponent<CriAtomSource>();
        animCycle = animCycle * Random.Range(0.5f, 2f);
    }

    // Update is called once per frame
    private void Update()
    {
        timer += Time.deltaTime;
        if (!escapeFlg)
        {
            if (timer >= animCycle)
            {
                timer = 0f;
                PlayIdleAnimation();
            }
        }
        else
        {
            Escape();
            if (timer >= deleteTiming)
                Destroy(gameObject);
        }
    }

    private void OnTriggerEnter(Collider other)
    {
        if (!escapeFlg)
        {
            //ダメージを受けると逃げるフラグがたつ
            if (other.gameObject.name == "KickCollision" ||
                other.gameObject.name == "MorningBlastSphere_Y(Clone)" ||
                other.gameObject.name == "Cutter(Clone)" ||
                other.gameObject.tag == "Chain" ||
                other.gameObject.name == "fallAttackCircle(Clone)")
            {
                EscapeContagion();
                criAtomSource.Play("Citizen00");
            }
            rb.isKinematic = false;
        }
    }

    private void PlayIdleAnimation()
    {
        //何もアニメーショントリガーが設定されてなければ無視
        if (animStrArray.Length != 0)
            animator.SetTrigger(animStrArray[Random.Range(0, animStrArray.Length)]);
    }
}