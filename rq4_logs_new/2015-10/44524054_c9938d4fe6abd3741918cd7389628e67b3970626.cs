using UnityEngine;
using System.Collections;

public class EnemyCreater : MonoBehaviour {

    [SerializeField, TooltipAttribute("クラス委員のプレハブを入れてください")]
    GameObject ClassCommittee = null;

    [SerializeField, TooltipAttribute("生徒会長のプレハブを入れてください")]
    GameObject StudentCouncilPresident = null;

    [SerializeField, TooltipAttribute("校長先生のプレハブを入れてください")]
    GameObject HeadMaster = null;

    public Vector3 enemyPosition { get { return gameObject.transform.position; } }

    // Use this for initialization
    void Start () {
        ClassCommitteeEenmyCreate();
    }
	
	// Update is called once per frame
	void Update () {
	
	}

  public  void ClassCommitteeEenmyCreate()
    {
        GameObject classCommitteeEenmyManager_ = GameObject.Find("EnemyManager"); 

        GameObject game_object = Instantiate(ClassCommittee);
        game_object.transform.SetParent(classCommitteeEenmyManager_.transform);
    }

    public void StudentCouncilPresidentEenmyCreate()
    {
        GameObject StudentCouncilPresidentEenmyManager_ = GameObject.Find("EnemyManager");

        GameObject game_object = Instantiate(StudentCouncilPresident);

        game_object.transform.SetParent(StudentCouncilPresidentEenmyManager_.transform);

    }

    public void HeadMasterEenmyCreate()
    {
        GameObject HeadMasterEenmyManager_ = GameObject.Find("EnemyManager");

        GameObject game_object = Instantiate(HeadMaster);

        game_object.transform.SetParent(HeadMasterEenmyManager_.transform);

    }

}