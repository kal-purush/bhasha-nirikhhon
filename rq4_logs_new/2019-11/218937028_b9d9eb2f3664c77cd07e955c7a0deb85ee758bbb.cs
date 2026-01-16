using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WalkingController : MonoBehaviour
{
    public float moveSpeed;
    public float rotSpeed;

    private bool isWandering = false;
    private bool isRotatingLeft = false;
    private bool isRotatingRight = false;
    private bool isWalking = false;
    private IEnumerator coroutine;
    void Update()
    {
        if (!isWandering) {
            coroutine = Wander();
            StartCoroutine(coroutine);
        }
        if (isWalking) {
            if (isInField()) {
                transform.position += transform.forward * Time.deltaTime * moveSpeed;
            } else {
                isRotatingLeft = true;
            }
        }
        if (isRotatingLeft) {
            transform.Rotate(transform.up * Time.deltaTime * rotSpeed);
        } else if (isRotatingRight) {
            transform.Rotate(transform.up * Time.deltaTime * -rotSpeed);
        }
    }

    IEnumerator Wander() {
        int rotTime = Random.Range(1, 2);
        int rotateWait = Random.Range(0, 1);
        int rotateLorR = Random.Range(0, 3);
        int walkWait = Random.Range(0, 1);
        int walkTime = Random.Range(1, 5);

        isWandering = true;

        yield return new WaitForSeconds(walkWait);
        isWalking = true;
        yield return new WaitForSeconds(walkTime);
        //isWalking = false;
        //yield return new WaitForSeconds(rotateWait);
        if(rotateLorR == 1) {
            isRotatingRight = true;
            yield return new WaitForSeconds(rotTime);
            isRotatingRight = false;
        } else if (rotateLorR == 2) {
            isRotatingLeft = true;
            yield return new WaitForSeconds(rotTime);
            isRotatingLeft = false;
        }

        isWandering = false;
    }

    private bool isInField() {
        float testX = transform.position.x + Time.deltaTime * moveSpeed;
        float textZ = transform.position.z + Time.deltaTime * moveSpeed;
        if (testX < 20 && -10 < testX && textZ < 20 && -10 < textZ) {
            return true;
        } else {
            return false;
        }
    }
}