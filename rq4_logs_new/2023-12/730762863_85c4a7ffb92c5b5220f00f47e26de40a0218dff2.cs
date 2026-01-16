using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MoveUpDown : MonoBehaviour
{
    [SerializeField] float distance;
    [SerializeField] float rotateDistance;
    [SerializeField] float rotateSpeed = 1f;
    [SerializeField] float speed = 1f;
    [SerializeField] bool rotate = false;
    Vector3 originalPosition;
    // Start is called before the first frame update
    float top, bottom;
    float rotateLeft, rotateRight;
    void Start()
    {
        originalPosition = transform.position;
        top = originalPosition.y + distance;
        bottom = originalPosition.y;
        rotateLeft = transform.rotation.z + rotateDistance;
        rotateRight = transform.rotation.z - rotateDistance;
        StartCoroutine(GoUp());
        if (rotate) StartCoroutine(RotateLeft());
    }

    IEnumerator GoUp()
    {
        while (true)
        {
            yield return null;
            var current = transform.position;
            if (current.y < top)
            {
                current.y += Time.deltaTime * speed;
                transform.position = current;
            }
            else
            {
                //current.y = originalPosition.y;
                //transform.position = current;
                break;
            }
        }
        StartCoroutine(GoDown());
    }
    IEnumerator GoDown()
    {
        while (true)
        {
            yield return null;
            var current = transform.position;
            if (current.y > bottom)
            {
                current.y -= Time.deltaTime * speed;
                transform.position = current;
            }
            else
            {
                //current.y = originalPosition.y;
                //transform.position = current;
                break;
            }
        }
        StartCoroutine(GoUp());
    }
    IEnumerator RotateLeft()
    {
        while (true)
        {
            yield return null;
            var current = transform.rotation;
            if (current.z < rotateLeft)
            {
                current.z += Time.deltaTime * rotateSpeed;
                transform.rotation = current;
                //transform.Rotate(new Vector3(0, 0, Time.deltaTime));
            }
            else
            {
                //current.y = originalPosition.y;
                //transform.position = current;
                break;
            }
        }
        StartCoroutine(RotateRight());
    }
    IEnumerator RotateRight()
    {
        while (true)
        {
            yield return null;
            var current = transform.rotation;
            if (current.z > rotateRight)
            {
                current.z -= Time.deltaTime * rotateSpeed;
                transform.rotation = current;
                //transform.Rotate(new Vector3(0, 0, Time.deltaTime));
            }
            else
            {
                //current.y = originalPosition.y;
                //transform.position = current;
                break;
            }
        }
        StartCoroutine(RotateLeft());
    }
    //private void OnCollisionEnter2D(Collision2D other)
    //{

    //    if (gameObject.CompareTag(Constant.TAG_ENEMY) && other.gameObject.CompareTag(Constant.TAG_BULLET))
    //    {
    //        Destroy(gameObject);
    //    }

    //}
}