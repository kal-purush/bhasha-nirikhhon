using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class NotificationSignal : MonoBehaviour {

    public GameObject panel;
    public ProfilePicture profilePicture;
    public Text title;
    public Text description;
    private bool isOn;
    public Animation anim;

	void Start () {
        SetOff();
        Invoke("EverySecond", 1);
	}
    public void SetOn(string facebookID, string username, string _description)
    {
        isOn = true;
        panel.SetActive(true);

        anim.Play("NotificationSignalOn");
        
        profilePicture.setPicture(facebookID);
        title.text = username.ToUpper();
        description.text = _description.ToUpper();
    }
    public void SetOff()
    {
        isOn = false;
        panel.SetActive(false);
    }
    void EverySecond()
    {
        if (!isOn && Application.loadedLevelName == "LevelSelector")
        {
            CheckForNotifications();
        }
        Invoke("EverySecond", 1);
    }
    void CheckForNotifications()
    {
        foreach( ChallengersManager.PlayerData playerData in Data.Instance.challengesManager.received)
        {
            if (!playerData.notificated && playerData.winner == "")
            {
                string text = "SENT YOU A NEW CHALLENGE!";
                SetOn(playerData.facebookID, playerData.playerName, text);                
                playerData.notificated = true;
                Events.OnChallengeNotificated(playerData.objectID);
                return;
            }
        }
        foreach (ChallengersManager.PlayerData playerData in Data.Instance.challengesManager.made)
        {
            if (!playerData.notificated && playerData.winner != "")
            {
                string text;
                if (playerData.winner != Data.Instance.userData.facebookID)
                    text = "accepted your challenge and beat you!";
                else
                    text = "accepted your challenge and lost!";

                SetOn(playerData.facebookID, playerData.playerName, text);
                playerData.notificated = true;
                Events.OnChallengeNotificated(playerData.objectID);
                return;
            }
        }
        foreach (Notifications.NotificationData data in Data.Instance.notifications.notifications)
        {
            if (!data.notificated && data.status == "0")
            {
                string text = "is requesting energy.";
                string username = Data.Instance.userData.GetUsernameByFacebookID(data.facebookID);
                SetOn(data.facebookID, username, text);
                data.notificated = true;
                Data.Instance.notifications.NotificationNotificated(data.facebookID, Data.Instance.userData.facebookID);
                return;
            }
        }
        foreach (Notifications.NotificationData data in Data.Instance.notifications.notificationsReceived)
        {
            if (!data.notificated && data.status == "1")
            {
                string text = "sent you energy!";
                string username = Data.Instance.userData.GetUsernameByFacebookID(data.asked_facebookID);
                SetOn(data.facebookID, username, text);
                data.notificated = true;
                Data.Instance.notifications.NotificationNotificated(Data.Instance.userData.facebookID, data.asked_facebookID);
                return;
            }
        }
    }
}