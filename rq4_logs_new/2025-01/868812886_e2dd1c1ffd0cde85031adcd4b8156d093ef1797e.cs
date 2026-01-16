using UnityEngine;
using UnityEngine.UI;

public class SoundController : MonoBehaviour
{
    public Slider musicSlider;

    void Start()
    {

        musicSlider.value = 1f;
    }

    void Update()
    {

        if (Input.GetKey(KeyCode.RightArrow))
        {
            AdjustSliderValue(0.01f);
        }
        if (Input.GetKey(KeyCode.LeftArrow))
        {
            AdjustSliderValue(-0.01f);
        }
    }

    void AdjustSliderValue(float adjustment)
    {
        musicSlider.value = Mathf.Clamp(musicSlider.value + adjustment, 0f, 1f);
    }
}