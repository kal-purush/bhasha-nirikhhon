using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DefaultCameraManager : MonoBehaviour
{
    static public DefaultCameraManager instance;
    public GameObject target; // 카메라가 따라갈 대상
    public float moveSpeed; // 카메라가 얼마나 빠른 속도로
    private Vector3 targetPosition; // target의 위치







    // Start is called before the first frame update 
    void Start()
    {
        if (instance == null)
        {
            DontDestroyOnLoad(this.gameObject);
            instance = this;
        }
        else
        {
            Destroy(this.gameObject);
        }
    }





    // Update is called once per frame
    public void Update()
    {
        if (target.gameObject != null)
        {
            targetPosition.Set(target.transform.position.x, this.transform.position.y, this.transform.position.z); // z값은 카메라 값으로

            this.transform.position = Vector3.Lerp(this.transform.position, targetPosition, moveSpeed * Time.deltaTime);
        }


    }
}
