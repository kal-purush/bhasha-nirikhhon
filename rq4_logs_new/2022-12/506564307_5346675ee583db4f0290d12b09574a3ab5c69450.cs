/*
遠方にある船をx方向に動かす
*/

using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DistantShip : MonoBehaviour
{
    [SerializeField]
    private float _speed;
    [SerializeField]
    private float _leftEnd;
    [SerializeField]
    private float _rightEnd;

    private float _time = 0.0f;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        this.transform.position += new Vector3(_speed, 0.0f, 0.0f) * Time.deltaTime;

        // 左端を超えたら、右端に移動
        if (this.transform.position.x > _leftEnd){
            this.transform.position = new Vector3(_rightEnd, this.transform.position.y, this.transform.position.z);
        }

        // 右端を超えたら、左端に移動
        if (this.transform.position.x < _rightEnd){
            this.transform.position = new Vector3(_leftEnd, this.transform.position.y, this.transform.position.z);
        }
    }
}