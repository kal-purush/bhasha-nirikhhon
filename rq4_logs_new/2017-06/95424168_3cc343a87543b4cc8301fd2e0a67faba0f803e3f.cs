using UnityEngine;
using System;
using System.Text;
using System.Net.Sockets;
using System.Collections;

public class ClientHandler : MonoBehaviour
{
  const int portNo = 7777;
  private TcpClient _client;
  byte[] data;
  public string nickName = "";
  public string message = "";
  public string sendMsg = "";
  bool isConnect=false;
  void OnGUI()
  {
    nickName = GUI.TextField(new Rect(20, 20, 200, 30), nickName);
    message = GUI.TextArea(new Rect(20, 60, 300, 200), message);
    sendMsg = GUI.TextField(new Rect(20, 270, 210, 30), sendMsg);
    if (!isConnect)
    {
      if (GUI.Button(new Rect(240, 20, 80, 40), "Connect") && !String.IsNullOrEmpty(nickName))
      {
        //Debug.Log("hello");
        this._client = new TcpClient();
        this._client.Connect("127.0.0.1", portNo);
        data = new byte[this._client.ReceiveBufferSize];
        //SendMessage(txtNick.Text);
        Send_Message(nickName);
        this._client.GetStream().BeginRead(data, 0, System.Convert.ToInt32(this._client.ReceiveBufferSize), ReceiveMessage, null);
        isConnect = true;
      };
    }
    if (GUI.Button(new Rect(230, 270, 80, 30), "Send"))
    {
      Send_Message(sendMsg);
      sendMsg = "";
    };
    if (isConnect)
    {
      if (GUI.Button(new Rect(340, 20, 80, 40), "DisConnect"))
      {
        this._client.Close();
        isConnect = false;
      } 
    }
  }
  public void Send_Message(string message)
  {
    try
    {
      NetworkStream ns = this._client.GetStream();
      byte[] data = System.Text.Encoding.UTF8.GetBytes(message);
      ns.Write(data, 0, data.Length);
      ns.Flush();
    }
    catch (Exception ex)
    {
      //MessageBox.Show(ex.ToString());
    }
  }
  public void ReceiveMessage(IAsyncResult ar)
  {
    try
    {
      int bytesRead;
      bytesRead = this._client.GetStream().EndRead(ar);
      if (bytesRead < 1)
      {
        return;
      }
      else
      {
        message += Encoding.UTF8.GetString(data, 0, bytesRead);//此处编码方式要对应否则乱码
      }
      this._client.GetStream().BeginRead(data, 0, System.Convert.ToInt32(this._client.ReceiveBufferSize), ReceiveMessage, null);
    }
    catch (Exception ex)
    {
    }
  }
}