using UnityEngine;
using UnityEngine.Rendering.PostProcessing;
using System.Collections;

public class PlayerPowerups : MonoBehaviour
{
    public static bool isSloMoActive = false;
    public static bool isMoAmmoActive = false;
    public static bool isMoDamageActive = false;
    public static bool isMoBulletsActive = false;

    public PostProcessVolume postProcessingVolume;
    private ChromaticAberration chromaticAberration;
    private ColorGrading colorGrading;

    private static Coroutine sloMoCoroutine;

    private void Start()
    {
        
        postProcessingVolume.profile.TryGetSettings(out chromaticAberration);
        postProcessingVolume.profile.TryGetSettings(out colorGrading);
    }

    public static void ActivateSloMo()
    {
        if (!isSloMoActive)
        {
            isSloMoActive = true;
            Time.timeScale = 0.5f;
            Time.fixedDeltaTime = 0.02f * Time.timeScale;

            if (sloMoCoroutine != null)
            {
                instance.StopCoroutine(sloMoCoroutine);
            }
            sloMoCoroutine = instance.StartCoroutine(SloMoTimer());
            instance.StartCoroutine(instance.ApplySloMoEffects(true));
        }
    }

    public static void ActivateMoAmmo()
    {
        isMoAmmoActive = true;
    }

    public static void ActivateMoDamage()
    {
        isMoDamageActive = true;
    }

    public static void ActivateMoBullets()
    {   
        isMoBulletsActive = true;
    }

    
    private static IEnumerator SloMoTimer()
    {
        yield return new WaitForSecondsRealtime(15f);
        DeactivateSloMo();
    }

    private static void DeactivateSloMo()
    {
        isSloMoActive = false;
        Time.timeScale = 1f;
        Time.fixedDeltaTime = 0.02f;
        instance.StartCoroutine(instance.ApplySloMoEffects(false));
    }

    
    private IEnumerator ApplySloMoEffects(bool activate)
    {
        float duration = activate ? 1f : 0.5f;
        float elapsedTime = 0f;

        while (elapsedTime < duration)
        {
            float t = elapsedTime / duration;
            float effectStrength = activate ? Mathf.Lerp(0f, 1f, t) : Mathf.Lerp(1f, 0f, t);
            
            chromaticAberration.intensity.value = effectStrength * 1f; 

            colorGrading.temperature.value = effectStrength * 40f; 
            colorGrading.tint.value = effectStrength * 10f; 

            elapsedTime += Time.unscaledDeltaTime;
            yield return null;
        }

        if (!activate)
        {
            chromaticAberration.intensity.value = 0f;
            colorGrading.temperature.value = 0f;
            colorGrading.tint.value = 0f;
        }
    }

    
    private static PlayerPowerups instance;

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }
    }
}