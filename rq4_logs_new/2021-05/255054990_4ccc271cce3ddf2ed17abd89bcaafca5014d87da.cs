using Sim;
using Sim.Interactables;
using TMPro;
using UnityEngine;

public class ElevatorUI : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private TMP_InputField searchTxt;

    [SerializeField]
    private AudioClip buttonClickSound;

    private Teleporter teleporterBind;

    private AudioSource audioSource;

    private void Awake() {
        this.audioSource = GetComponent<AudioSource>();
    }

    public void Bind(Teleporter bind) {
        this.teleporterBind = bind;
        this.searchTxt.text = string.Empty;
    }

    public void AddNumber(int value) {
        this.audioSource.PlayOneShot(this.buttonClickSound);

        if (this.searchTxt.text.Length < 2) {
            this.searchTxt.text += value.ToString();
        }
    }

    public void Confirm() {
        this.audioSource.PlayOneShot(this.buttonClickSound);

        if (int.TryParse(this.searchTxt.text, out var floorToGo)) {
            this.teleporterBind.CmdUse(floorToGo);
            DefaultViewUI.Instance.HideElevatorUI();
        }
    }

    public void GoToHall() {
        this.audioSource.PlayOneShot(this.buttonClickSound);
        this.teleporterBind.CmdUse(0);
        DefaultViewUI.Instance.HideElevatorUI();
    }

    public void Abort() {
        DefaultViewUI.Instance.HideElevatorUI();
    }
}