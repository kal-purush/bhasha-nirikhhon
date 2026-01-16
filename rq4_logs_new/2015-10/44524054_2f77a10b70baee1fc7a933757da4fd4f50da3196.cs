
using UnityEngine;


public class BackGroundSetting : MonoBehaviour {

  [SerializeField]
  GameObject[] _stageText = null;


  void Start() {
    _stageText[0] = GameObject.Find("MainStage");
    _stageText[1] = GameObject.Find("LastStage");
  }

  public void StageChange(uint stageNum) {
    if (_stageText[stageNum].activeInHierarchy) { return; }

    // 値が配列の範囲外なら範囲内に収める
    stageNum = stageNum / (uint)_stageText.Length;

    for (uint index = 0; index < _stageText.Length; ++index) {
      var isActive = (index == stageNum);
      _stageText[index].SetActive(isActive);
    }
  }
}