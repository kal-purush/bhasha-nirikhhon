using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SoundCont : MonoBehaviour
{
    public float BGMVol;
    public float SEVol;
    public Slider BGMSlider;
    public Slider SESlider;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }

    public void BGMSet()
    {
        BGMVol = BGMSlider.value;
        CriAtom.SetCategoryVolume("Music", BGMVol);
    }

    public void SESet()
    {
        SEVol = SESlider.value;
        CriAtom.SetCategoryVolume("SFX", SEVol);
    }
}