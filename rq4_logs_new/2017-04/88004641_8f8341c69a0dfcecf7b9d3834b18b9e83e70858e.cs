using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SwitchTag : MonoBehaviour {
	[SerializeField] private GameObject _recluitPanel;
	[SerializeField] private GameObject _trainingPanel;
	[SerializeField] private Button _recluitButton;
	[SerializeField] private Button _trainingButton;
	[SerializeField] private Button _closeButton;

	void Start ()
	{
		_recluitButton.onClick.AddListener (() => {
			OpenRecluitTag();
        });

		_trainingButton.onClick.AddListener (() => {
            OpenTrainingTag();
        });

		_closeButton.onClick.AddListener (() => {
             CloseTag();
        });
	}

	// Recluitタグを開くメソッド
	void OpenRecluitTag ()
	{
		if (_trainingPanel.activeSelf)
		{
			_trainingPanel.SetActive(false);
		}
		_recluitPanel.SetActive(true);
	}

	// Trainingタグを開くメソッド
	void OpenTrainingTag ()
	{
		if (_recluitPanel.activeSelf)
		{
			_recluitPanel.SetActive(false);
		}
		_trainingPanel.SetActive(true);
	}

	// タグを閉じるメソッド
	void CloseTag ()
	{
		if (_trainingPanel.activeSelf || _recluitPanel.activeSelf)
		{
			_trainingPanel.SetActive(false);
			_recluitPanel.SetActive(false);
		}
	}
}