using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMov : MonoBehaviour
{
    public GameObject target;
    public float moveSpeed;
    [Tooltip("Posição relativa entre camera e alvo. Mantenha um Z negativo")]
    private Transform transfPlayer;
    // Start is called before the first frame update
    void Start()
    {

        transfPlayer = target.GetComponent<Transform>();
    }
    // Update is called once per frame
    void LateUpdate()
    {
        //A: move-se junto com o player 
        transform.position = Vector3.Lerp(transform.position, transfPlayer.position + new Vector3(0, 0, -20), moveSpeed * Time.deltaTime);
    }
}