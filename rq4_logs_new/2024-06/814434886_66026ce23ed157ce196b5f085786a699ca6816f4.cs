using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class player : MonoBehaviour
{

    private GameObject clickedGameObject;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
        if(Input.GetMouseButton(0))
        {

            clickedGameObject = null;

            //���C���J��������������o��
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);

            //�����ɂ��������I�u�W�F�N�g���擾
            RaycastHit2D hit2D = Physics2D.Raycast((Vector2)ray.origin, (Vector2)ray.direction);

            if(hit2D)
            {

                //�����ɂ��������I�u�W�F�N�g��GameObject���擾����
                clickedGameObject = hit2D.transform.gameObject;
            }

            Debug.Log(clickedGameObject);
        }
    }
}