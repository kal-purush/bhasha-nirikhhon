using System.Collections;
using System.Collections.Generic;
using UnityEngine;


[RequireComponent(typeof(Rigidbody))]

public class Ball : MonoBehaviour
{

   /// <summary>
   /// 物理剛体
   /// </summary>
    private Rigidbody physics = null;

    /// <summary>
    /// 初期化処理
    /// </summary>
    public void Awake()
    {
        this.physics = this.GetComponent<Rigidbody>();
    }

    // Start is called before the first frame update
    void Start()
    {
        
    }

    /// <summary>
    /// 毎フレームごとの処理
    /// </summary>
    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            this.Flip(new Vector3(3f, 6f, 0));
        }
    }

    /// <summary>
    /// ボールをはじく
    /// </summary>
    /// <param name="force"></param>
    public void Flip(Vector3 force)
    {
        //瞬時に力を加えてはじく
        this.physics.AddForce(force, ForceMode.Impulse);
    }

    ///<summary>
    /// 当たり判定
    /// </summary>
    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.tag == "Goal")
        {
            Debug.Log("おわり");
        }
    }
}