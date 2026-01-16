using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MentorRecluitItemManager : MonoBehaviour {

[SerializeField] private MasterDataManager _masterDataManager;
[SerializeField] private GameObject _mentorRecluitItem;
[SerializeField] private GameObject _mentorRecluitItemContent;

private List<MstCharacter> _characterTable;

	void Start ()
	{

		// MasterDataManagerにマッピングされたデータを表示していく
		 SetUpAllMentorRecluitItem();
	}

	private void SetUpAllMentorRecluitItem ()
	{
		// キャラクターデータのListを入手
		_characterTable = _masterDataManager.CharacterTable;

		// foreach文回す時のindexを定義
		foreach (MstCharacter chara in _characterTable)
		{
			// itemにInstantiateしたMentorRecluitItemを入れる
			GameObject item = Instantiate(
								  _mentorRecluitItem,
								  _mentorRecluitItemContent.transform.position,
								  Quaternion.identity,
								  _mentorRecluitItemContent.transform
							  );

			// itemのMentorRecruitItemBehaviourを取得して値を入れていく
			MentorRecruitItemBehaviour mentorRecruitItemBehaviour = item.GetComponent<MentorRecruitItemBehaviour>();
			mentorRecruitItemBehaviour.SetValues(chara);
			mentorRecruitItemBehaviour.SetButtonFunction();
		}
	}

}