using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerShape : MonoBehaviour {
    // 私有属性
    private float[] realPos = { -1.1f, 0, 1.1f }; // 三个位置的x坐标
    private string[] pos = { "left", "middle", "right" }; // 三个位置
    private string currentPos; // 几何体当前位置
    private int currentPosIndex; // 几何体当前位置抽象数字
    private float speed = 4f; // 几何体移动速度
    private int direction; // 方向

    // 精灵组件

	// Use this for initialization
	void Start () {
        // 位置初始化
        currentPos = "middle";
        currentPosIndex = 1;
	}
	
	// Update is called once per frame
	void Update () {
        moving();
	}

    // 玩家移动几何体的方法
    private void moving()
    {
        if (Input.GetKeyDown(KeyCode.RightArrow))
            direction = 1;
        else if (Input.GetKeyDown(KeyCode.LeftArrow))
            direction = -1;

        if ((direction == 1) && (currentPos == "left" || currentPos == "middle"))
        {
            transform.position += Vector3.right * speed * Time.deltaTime;
            if (transform.position.x >= realPos[currentPosIndex + 1])
            {
                currentPosIndex++;
                transform.position = new Vector3(realPos[currentPosIndex], transform.position.y, transform.position.z);
                currentPos = pos[currentPosIndex];
                direction = 0;
            }
        }
        else if ((direction == -1) && (currentPos == "right" || currentPos == "middle"))
        {
            transform.position += Vector3.left * speed * Time.deltaTime;
            if (transform.position.x <= realPos[currentPosIndex - 1])
            {
                currentPosIndex--;
                transform.position = new Vector3(realPos[currentPosIndex], transform.position.y, transform.position.z);
                currentPos = pos[currentPosIndex];
                direction = 0;
            }
        }
    }
}