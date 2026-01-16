
using UnityEngine;


public class EnemyManager : MonoBehaviour {

  uint stageNum = 0;

  BackGroundSetting back = null;
  StageNameSetting name = null;

  EnemyCreater enemyManager = null;


  void Start() {
    back = FindObjectOfType<BackGroundSetting>();
    name = FindObjectOfType<StageNameSetting>();
    back.StageChange(stageNum);
    name.StageChange(stageNum);

    enemyManager = FindObjectOfType<EnemyCreater>();
    switch (stageNum) {
      case 0: enemyManager.ClassCommitteeEenmyCreate();
        break;
      case 1: enemyManager.StudentCouncilPresidentEenmyCreate();
        break;
      case 2:
        break;
      default:
        break;
    }

    var audio = FindObjectOfType<AudioManager>();
    audio.PlayBGM(0);
  }

  void Update() {
  }
}