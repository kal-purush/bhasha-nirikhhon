using UnityEngine;

public class Cutscene4 : MonoBehaviour
{
    private bool hasCutsceneStarted = false;

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            CutsceneManager.Instance.PlayCutscene(3);
            hasCutsceneStarted = true;
        }
    }

    void Update()
    {
        if (hasCutsceneStarted && !CutsceneManager.Instance.isCutscenePlaying)
        {
            gameObject.SetActive(false);
            hasCutsceneStarted = false; // Đảm bảo không chạy mã này nhiều lần
        }
    }
}