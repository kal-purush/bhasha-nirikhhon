using UnityEngine;
using UnityEngine.Rendering.PostProcessing;

public class BloomEffect : MonoBehaviour
{
    public PostProcessVolume volume;
    private Bloom _bloom;
    bool isBloomActive;

    void Start()
    {
        isBloomActive = false;
        volume.profile.TryGetSettings(out _bloom);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.F) && (isBloomActive == false)) 
        {
            Debug.Log("Activando Bloom");
            _bloom.intensity.value = 25;
            isBloomActive = true;
        }
        else if (Input.GetKeyDown(KeyCode.F) && (isBloomActive == true))
        {
            Debug.Log("Desactivando Bloom");
            _bloom.intensity.value = 0;
            isBloomActive = false;
        }
    }
}
 