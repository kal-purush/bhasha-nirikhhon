using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;
using Photon.Realtime;

public class BodyUp : MonoBehaviourPunCallbacks
{
    public GameObject bodyUp;
    private bool _isBodyUp = false;
    private string _isBodyUpTag;
    private bool _isObjectTouch = false;
    private Rigidbody upRb;

    private void Start()
    {
        upRb = GetComponent<Rigidbody>();
    }

    // Update is called once per frame
    private void Update()
    {
        if(_isBodyUp && _isObjectTouch && (_isBodyUpTag == "BodyUp")) {
            if (photonView.IsMine) {
                upRb.isKinematic = true;
                Transform bodyTransform = bodyUp.transform;
                Vector3 bodyPos = bodyTransform.position;
                bodyPos.y = 0.35f;
                bodyTransform.position = bodyPos;

                _isObjectTouch = false;
            }
        } else if(_isBodyUp && _isObjectTouch && (_isBodyUpTag == "BodyUp1")) {
            if (photonView.IsMine) {
                upRb.isKinematic = true;
                Transform bodyTransform = bodyUp.transform;
                Vector3 bodyPos = bodyTransform.position;
                bodyPos.y = 0.7f;
                bodyTransform.position = bodyPos;

                _isObjectTouch = false;
            }
        }　else if(_isBodyUp && _isObjectTouch && (_isBodyUpTag == "BodyUp2")) {
            if (photonView.IsMine) {
                upRb.isKinematic = true;
                Transform bodyTransform = bodyUp.transform;
                Vector3 bodyPos = bodyTransform.position;
                bodyPos.y = 1.05f;
                bodyTransform.position = bodyPos;

                _isObjectTouch = false;
            }
        } else if(_isBodyUp && _isObjectTouch && (_isBodyUpTag == "BodyUp3")) {
            if (photonView.IsMine) {
                upRb.isKinematic = true;
                Transform bodyTransform = bodyUp.transform;
                Vector3 bodyPos = bodyTransform.position;
                bodyPos.y = 1.4f;
                bodyTransform.position = bodyPos;

                _isObjectTouch = false;
            }
        }

        if(!_isBodyUp && !_isObjectTouch) {
            if (photonView.IsMine) {
                upRb.isKinematic = false;
                Transform bodyTransform = bodyUp.transform;
                Vector3 bodyPos = bodyTransform.position;
                bodyPos.y = 0f;
                bodyTransform.position = bodyPos;

                _isObjectTouch = true;
            }
        }
    }

    private void OnTriggerEnter(Collider other)
    {
        if (photonView.IsMine) {
            _isBodyUpTag = other.gameObject.tag;
            _isBodyUp = true;
        }
    }


    private void OnTriggerExit(Collider other)
    {
        //離れたオブジェクトのタグが"Player"のとき
        if (photonView.IsMine) {
            _isBodyUpTag = other.gameObject.tag;
            _isBodyUp = false;
        }
    }
}