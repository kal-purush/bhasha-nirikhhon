using UnityEngine;

public class ForbiddenTarget : MonoBehaviour
{
    [Header("Penalty Settings")]
    public int penaltyScore = 30; // 減点するスコア
    public AudioClip errorSFX;    // 誤爆時のSE
    [Range(0f, 10f)] public float sfxVolume = 1.0f;

    public float destroyTime = 5f;

    private void Start()
    {
        // 時間が経ったら自然に消える（何もしない）
        Invoke(nameof(AutoDestroy), destroyTime);
    }

    private void AutoDestroy()
    {
        // 何もせずに消滅（ペナルティなし）
        Destroy(gameObject);
    }

    private void OnMouseDown()
    {
        // コンボリセット
        ScoreManager.Instance.ResetCombo();

        // スコア減算（最低0まで）
        ScoreManager.Instance.score -= penaltyScore;
        if (ScoreManager.Instance.score < 0)
            ScoreManager.Instance.score = 0;

        Debug.Log($"禁止ターゲットを破壊！スコア -{penaltyScore}、コンボリセット");

        PlaySound(errorSFX);

        // UI更新：AddScoreを使わずUpdateで表示のみリフレッシュ
        UIManager.Instance?.UpdateScoreText();
        UIManager.Instance?.UpdateCombo(0);

        Destroy(gameObject);
    }



    private void OnDestroy()
    {
        CancelInvoke(nameof(AutoDestroy));
    }

    private void PlaySound(AudioClip clip)
    {
        if (clip == null) return;

        GameObject tempGO = new GameObject("TempErrorSFX");
        AudioSource aSource = tempGO.AddComponent<AudioSource>();

        aSource.clip = clip;
        aSource.volume = sfxVolume;
        aSource.spatialBlend = 0f;
        aSource.Play();

        Destroy(tempGO, clip.length);
    }
}