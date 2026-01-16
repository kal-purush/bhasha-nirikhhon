using UnityEngine;
using UnityEngine.SceneManagement;
using System.Collections;

public class FadeManager : Singleton<FadeManager> {

    private Texture2D blackTex;

    private float fadeAlpha = 0;
    private bool isFading = false;

    public void Awake()
    {
        if (this != Instance)
        {
            Destroy(this);
            return;
        }

        DontDestroyOnLoad(this.gameObject);


        this.blackTex = new Texture2D(32, 32, TextureFormat.RGB24, false);
        this.blackTex.SetPixel(0, 0, Color.white);
        this.blackTex.Apply();
    }

    public void OnGUI()
    {
        if (!this.isFading)
            return;
        GUI.color = new Color(0, 0, 0, this.fadeAlpha);

        GUI.DrawTexture(new Rect(0, 0, Screen.width, Screen.height), this.blackTex);
    }

    public void LoadScene(string sceneName,float interval)
    {
        StartCoroutine(SceneShift(sceneName, interval));
    }

    IEnumerator SceneShift(string scene,float interval)
    {
        this.isFading = true;
        float timer = 0;
        // 徐々に暗くする
        while (timer <= interval)
        {
            this.fadeAlpha = Mathf.Lerp(0.0f, 1.0f, timer / interval);
            timer += Time.deltaTime;
            yield return 0;
        }

        SceneManager.LoadScene(scene);

        timer = 0;
        // 徐々に明るくする
        while(timer <= interval)
        {
            this.fadeAlpha = Mathf.Lerp(1.0f, 0.0f, timer / interval);
            Time.timeScale = 1.0f;
            timer += Time.deltaTime;
            yield return 0;
        }

        this.isFading = false;
    }

}