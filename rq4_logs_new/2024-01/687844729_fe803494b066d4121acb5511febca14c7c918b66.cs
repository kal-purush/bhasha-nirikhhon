using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class UIManager : MonoBehaviour
{
    private RhythmManager rm;

    public Text SongInfo;
    public Text score;
    public Text combo;

    public void Start()
    {
        rm = GameManager.myManager.rm;
    }
    public void InitiateUI() {
        SongInfo.text = ""; // 맵 파일에서 곡 정보 저장해야 한다.
        score.text = "0";
        combo.text = "";

    }

    public void UpdateUI() {
        score.text = rm.score.ToString();
        combo.text = rm.combo.ToString();
    }
}