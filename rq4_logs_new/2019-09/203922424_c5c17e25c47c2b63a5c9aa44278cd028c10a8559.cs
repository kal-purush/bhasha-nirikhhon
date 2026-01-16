using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class button : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {//判断是否是点击事件			 
            Ray ray = Camera.main.ScreenPointToRay (Input.mousePosition);
            RaycastHit hitInfo;
            if (Physics.Raycast(ray,out hitInfo) ){
                //如果是一根手指触摸屏幕而且是刚开始触摸屏幕 					
                if (Input.touchCount==1&&Input.GetTouch(0).phase==TouchPhase.Began){
                    if (Input.GetTouch(0).tapCount==1){//判断点击的次数						
                        Destroy (hitInfo.collider.gameObject);//销毁场景中的模型					
                    }
                }
            }		}	

    }
}