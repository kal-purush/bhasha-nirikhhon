using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CameraManager : MonoBehaviour {
	public static CameraManager instance;
	[SerializeField] private GameObject mainCamera;
	[SerializeField] private GameObject normalStateUI;
	[SerializeField] private GameObject vrViewUI;
	[SerializeField] private Button backButton;
	private GameObject avator;

	void Start ()
	{
		// singleton化
		if (instance == null)
		{
			instance = this;
			DontDestroyOnLoad (instance.gameObject);
		}
	}
	public void SwitchFromMainToVive (GameObject _avator)
	{
		// avatorをクラス変数に格納
		avator = _avator;

		// ボタンに処理を記述
		backButton.onClick.AddListener (() => {
			avator.GetComponent<MentorAvatorBehavior>().FinishVRView();
        });

		// 通常画面用のGUIを無効にする
		normalStateUI.SetActive(false);

		// MainCameraを無効にする
		mainCamera.SetActive(false);

		// mentorに紐づいたDiveCameraを有効にする
		avator.GetComponent<MentorAvatorBehavior>().DiveCamera.SetActive(true);

		// VR View画面用のGUIを有効にする
		vrViewUI.SetActive(true);
	}

	public void SwitchFromDiveToMain ()
	{
		// VR View画面用のGUIを無効にする
		vrViewUI.SetActive(false);

		//現在有効になっているカメラを無効にする
		avator.GetComponent<MentorAvatorBehavior>().DiveCamera.SetActive(false);

		// MainCameraを有効にする
		mainCamera.SetActive(true);

		// 通常画面用のGUIを有効にする
		normalStateUI.SetActive(true);

		// クラス変数をリセット
		avator = null;
	}

	public void SwitchFromMainToMentor (GameObject _avator)
	{
		// avatorをクラス変数に格納
		avator = _avator;

		// ボタンに処理を記述
		backButton.onClick.AddListener (() => {
			avator.GetComponent<MentorAvatorBehavior>().FinishVRView();
        });

		// 通常画面用のGUIを無効にする
		normalStateUI.SetActive(false);

		// MainCameraを無効にする
		mainCamera.SetActive(false);

		// mentorに紐づいたDiveCameraを有効にする
		avator.GetComponent<MentorAvatorBehavior>().DiveCamera.SetActive(true);

		// VR View画面用のGUIを有効にする
		vrViewUI.SetActive(true);
	}
}