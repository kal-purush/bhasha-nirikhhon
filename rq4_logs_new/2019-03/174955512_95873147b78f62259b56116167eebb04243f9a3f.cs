using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexShake : MonoBehaviour
{
    Vector3 basePos;
    RShake shake;
    // Start is called before the first frame update
    void Start()
    {
        basePos = transform.localPosition;
        shake = new RShake();
    }

    public void Shake(float pow){
        shake.SetShake(pow);
    }

    public void ShakeByChain(int chainNum){
        if (chainNum <= 1) return;
        if (chainNum <= 3) {
            Shake(5);
            return;
        }
        if (chainNum <= 5) {
            Shake(10);
            return;
        }
        Shake(20);
    }

    // Update is called once per frame
    void Update()
    {
        transform.localPosition = basePos + shake.Delta();
    }
}