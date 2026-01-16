using System.Collections;
using System.Collections.Generic;
using UnityEngine;



public class ComboController : MonoBehaviour
{


    private float tempTime;
    void Start()
    {
        tempTime = 0;


        //获取材质本来的属性  
        this.GetComponent<Renderer>().material.color = new Color
        (
                this.GetComponent<Renderer>().material.color.r,
                this.GetComponent<Renderer>().material.color.g,
                this.GetComponent<Renderer>().material.color.b,
                //需要改的就是这个属性：Alpha值  
                this.GetComponent<Renderer>().material.color.a
        );
    }
    void Update()
    {
        if (tempTime < 1)
        {
            tempTime = tempTime + Time.deltaTime;
        }
        if (this.GetComponent<Renderer>().material.color.a <= 1)
        {
            this.GetComponent<Renderer>().material.color = new Color
            (
                this.GetComponent<Renderer>().material.color.r,
                this.GetComponent<Renderer>().material.color.g,
                this.GetComponent<Renderer>().material.color.b,


            //减小Alpha值，从1-30秒逐渐淡化 ,数值越大淡化越慢 
            gameObject.GetComponent<Renderer>().material.color.a - tempTime / 30 * Time.deltaTime
            );
        }
        Destroy(this.gameObject, 40.0f);//40秒后消除
    }
}