using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VolumeControl : MonoBehaviour
{
    // Start is called before the first frame update
    public UnityEngine.UI.Slider UsevolSlider;
    public string SoundCategory;
    private CriAtomSource atomSrc;
    // Start is called before the first frame update
    void Start()
    {
        atomSrc = (CriAtomSource)GetComponent("CriAtomSource");
        //Slider操作で音量が変わる
        UsevolSlider.onValueChanged.AddListener(delegate { ValueChangeCheck(); });
        //Sliderの値を音量と同期させる
        UsevolSlider.value = CriAtom.GetCategoryVolume(SoundCategory);
    }
    // Update is called once per frame
    void Update()
    {
    }
    public void PlaySound()
    {
        if (atomSrc != null)
        {
            atomSrc.Play();
        }
    }
    public void PlayAndStopSound()
    {
        if (atomSrc != null)
        {
            CriAtomSource.Status status = atomSrc.status;
            if ((status == CriAtomSource.Status.Stop) || (status == CriAtomSource.Status.PlayEnd))
            {
                atomSrc.Play();
            }
            else
            {
                atomSrc.Stop();
            }
        }
    }
    /* イベントコールバック用関数を追加 */
    public void ValueChangeCheck()
    {
        //音量をSliderの値にする
        CriAtom.SetCategoryVolume(SoundCategory, UsevolSlider.value);
    }
}

