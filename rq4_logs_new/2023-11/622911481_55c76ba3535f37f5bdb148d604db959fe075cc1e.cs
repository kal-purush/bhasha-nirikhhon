using System.Collections;
using System.Collections.Generic;
using Enums;
using TMPro;
using UnityEngine;
using UnityEngine.Events;
using UnityEngine.UI;

public class ReactionGameQuestUI : MonoBehaviour
{
    [SerializeField] private GameObject _questPanel;
    [SerializeField] private QuestObject _questObject;
    [SerializeField] private PlayerController _playerController;
    [SerializeField] private GameObject _player;

    [Header("Header")]
    [SerializeField] private TextMeshProUGUI _titleField;

    [Header("Body")]
    [SerializeField] private Transform _bodyArea;
    [Space()]
    [SerializeField] private TextMeshProUGUI _descriptionText;
    [Space()]

    [Header("Footer")]
    [SerializeField] private Transform _footerArea;
    [SerializeField] private TMP_InputField _inputField;
    [SerializeField] private Button _confirmButton;
    [SerializeField] private Button _cancelButton;
    [SerializeField] private TextMeshProUGUI _footerText;

    [Header("Events")]
    public UnityEvent onContinueCallback;

    private void Awake()
    {
        Close();
        _titleField.text = string.Format("Quest - {0}", _questObject.QuestName);
        _descriptionText.text = _questObject.QuestDescription;
        _confirmButton.onClick.AddListener(OnConfirm);
        _cancelButton.onClick.AddListener(Close);
    }

    public void Close()
    {
        _questPanel.SetActive(false);
        // Enable Playermovement
        // TODO: Fix this
    }

    public void Open()
    {
        _footerText.text = "Achtung! Wenn du falsche Antworten gibst, wird dir Strafzeit berechnet.";
        _questPanel.SetActive(true);
        // Disable Playermovement
        // TODO: Fix this
    }

    public void OnConfirm()
    {
        if (_questObject.CheckAnswer(_inputField.text))
        {
            onContinueCallback?.Invoke();
            SaveObject x = SaveLoad.Instance.saveObject;
            switch (_questObject.QuestName)
            {
                case "Brücke":
                    x.TaskBrokenBridge = 2;
                    break;
                case "Aufbruch":
                    x.TaskBoatOpened = 2;
                    break;
                case "Schuhe":
                    x.TaskShoesCollected = 2;
                    break;
                case "Kaputtes Schiff":
                    x.TaskHoleFixed = 2;
                    break;
                case "Waldpfad":
                    x.PathToEvilVillage = 2;
                    break;
            }
            Close();
        }
        else
        {
            _footerText.text = string.Format("<u>{0}</u> ist leider falsch. Versuche es nochmal.", _inputField.text == string.Empty ? "Nichts..." : _inputField.text);
            // Penalties
            if(_inputField.text != string.Empty) {
                if(!_questObject.AnswersGiven.Contains(_inputField.text)) {
                    _questObject.AnswersGiven.Add(_inputField.text);
                    // TODO: Add penalty
                }
            }
        }
    }
}