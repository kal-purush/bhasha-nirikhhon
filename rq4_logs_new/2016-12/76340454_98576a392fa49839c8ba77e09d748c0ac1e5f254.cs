using UnityEngine;
using UnityEngine.UI;
using System.Collections;


public enum ButtonType
{
    START,
    EXPLAIN,
    OPTIONTOTITLE,
}
/*UI動作時の処理を記述します*/
public class TitleUIController : MonoBehaviour {
    // Scene>TitleのCanvasを全て取得
    Canvas TitleCanvas, OptionCanvas;


    // TitleCanvas関連
    Button StartButton, ExplanationButton;


    // OptionCanvas関連
    // SoundManagerのキャッシュ
    private SoundManager soundManager;
    // SoundVolumeクラス これをSoundManagerのvolumeに設定する
    private SoundVolume soundVolume;
    Slider BGMSlider, SESlider;
    Button ToTitleButton;


	// Use this for initialization
	void Start () {
        // Canvas取得
        TitleCanvas = GameObject.Find("TitleCanvas").GetComponent<Canvas>();
        OptionCanvas = GameObject.Find("OptionCanvas").GetComponent<Canvas>();

        //Title関連の参照を取得
        StartButton = GameObject.Find("StartButton").GetComponent<Button>();
        ExplanationButton = GameObject.Find("ExplanationButton").GetComponent<Button>();

        // Option関連の参照を取得
        soundManager = SoundManager.Instance;
        soundVolume = new SoundVolume();
        BGMSlider = GameObject.Find("BGMSlider").GetComponent<Slider>();
        SESlider = GameObject.Find("SESlider").GetComponent<Slider>();
	}

    public void OnButtonClicked(ButtonType type)
    {
        switch (type)
        {
            case ButtonType.START:

                break;
            case ButtonType.EXPLAIN:
                break;
            case ButtonType.OPTIONTOTITLE:
                OptionCanvas.enabled = true;
                break;
            default:
                Debug.LogError("ButtonType not Attached!!");
                return;
        }
    }



    /// <summary>
    /// OptionCanvas>Sliders
    /// 
    /// SoundVolumeより音量を設定するクラス
    /// </summary>
    public void SetVolume()
    {
        this.soundVolume.BGMVolume = this.BGMSlider.value;
        this.soundVolume.SEVolume = this.SESlider.value;
        this.soundManager.volume = this.soundVolume;
        Debug.Log("BGM : " + this.soundVolume.BGMVolume + "\nSE : " + this.soundVolume.SEVolume);
    }


}