using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class ChooseTankHandler : MonoBehaviour
{
    public Button btnSendMessage;
    [SerializeField]
    private Text txtCountTime;
    private float timeRemaining;

    public GameObject chatPanel, textObject;
    public InputField chatBox;
    public Color playerMessage, info;

    private int maxMessages = 25;

    [SerializeField]
    private List<Message> messageList = new List<Message>();
    private string username = "Kingwisdom";

    // Start is called before the first frame update
    void Start()
    {
        btnSendMessage.onClick.AddListener(SendPlayerMessage);
        timeRemaining = float.Parse(txtCountTime.text);
    }

    // Update is called once per frame
    void Update()
    {
        // count time and move to next sceme
        timeRemaining -= Time.deltaTime;
        txtCountTime.text = Mathf.CeilToInt(timeRemaining).ToString();
        if (timeRemaining <= 0)
        {
            SceneManager.LoadScene("WaittingMatch");
        }

        // box chat

        if (chatBox.text != "")
        {
            if (Input.GetKeyDown(KeyCode.Return))
            {
                SendPlayerMessage();
            }
        } else if (!chatBox.isFocused && Input.GetKeyDown(KeyCode.Return))
        {
            chatBox.ActivateInputField();
        }
    }

    private void SendPlayerMessage()
    {
        SendMessageToChat(username + ": " + chatBox.text, Message.messageType.playerMessage);
        chatBox.text = "";
    }

    private void SendMessageToChat(string text, Message.messageType messageType)
    {
        if (messageList.Count >= maxMessages)
        {
            Destroy(messageList[0].textObject.gameObject);
            messageList.Remove(messageList[0]);
        }
        Message newMessage = new Message();
        newMessage.text = text;

        // init new Text Object inside chatPanel
        GameObject newText = Instantiate(textObject, chatPanel.transform);
        newMessage.textObject = newText.GetComponent<Text>();
        newMessage.textObject.text = newMessage.text;
        newMessage.textObject.color = MesssageTypeColor(messageType);

        messageList.Add(newMessage);
    }

    private Color MesssageTypeColor(Message.messageType messageType)
    {
        Color color = playerMessage;
        switch (messageType)
        {
            case Message.messageType.playerMessage:
                color = new Color(playerMessage.r, playerMessage.g, playerMessage.b);
                break;
            case Message.messageType.info:
                color = new Color(info.r, info.g, info.b);
                break;
        }
        return color;
    }

}

[System.Serializable]
public class Message
{
    public string text;
    public Text textObject;
    public enum messageType
    {
        playerMessage,
        info
    }
}