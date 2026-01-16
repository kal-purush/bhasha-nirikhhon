using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class raycast : MonoBehaviour {

    void Update () {

        RaycastHit pontoColisao;

        if (Input.GetKey(KeyCode.H)) {
            if (Physics.Raycast (transform.position, transform.forward, out pontoColisao, 20)) {
                if (pontoColisao.transform.gameObject.GetComponent<MeshRenderer> () != null) {
                    pontoColisao.transform.gameObject.GetComponent<MeshRenderer> ().enabled = false;
                }
            } else {
                Debug.Log ("Não está colidindo");
            };
        }

    }
}