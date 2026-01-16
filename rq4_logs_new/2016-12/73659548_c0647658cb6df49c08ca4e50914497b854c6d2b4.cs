using UnityEngine;
using System.Collections;

public class FrameRate : MonoBehaviour
{

    void Awake()
    {
        QualitySettings.vSyncCount = 0; // VSyncをOFFにする
        Application.targetFrameRate = 60; // ターゲットフレームレートを60に設定
    }

    void Update()
    {

    }
}