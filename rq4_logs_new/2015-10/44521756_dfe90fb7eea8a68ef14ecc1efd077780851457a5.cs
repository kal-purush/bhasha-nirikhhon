using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public enum RoomSceneState {
	Default,
	Play,
	MyTurn,
	Move,
	Wait,
	ThrowWait,
}

public class RoomScene : MonoBehaviour {

	public TcpClient Tcp;
	public Text txtPlayer1;
	public Text txtPlayer2;
	public Text txtMain;

	public RoomSceneState State = RoomSceneState.Default;

	public void SetMyTurn() {
		State = RoomSceneState.MyTurn;
		txtMain.text = "Please, Input Space Bar...";
	}

	public void SetWait() {
		State = RoomSceneState.Wait;
		txtMain.text = "Please, Wait...";
	}

	void Update() {
		if(State == RoomSceneState.MyTurn) {
			if(Input.GetKeyDown(KeyCode.Space)) {
				Debug.Log("spacebar down");
				txtMain.text = "Dice...";
				State = RoomSceneState.ThrowWait;
				JJSocket sock = new JJSocket();
				sock.type = JJSocketType.RoomThrowDice;
				Tcp.SocketManager.tcpSocket.SendMessage(sock);
			}
		}
	}

	public void Move(SocketRoomThrowDice dice) {
		Debug.Log("t:" + dice.t + " d1:" + dice.d1 + " d2:" + dice.d2);
		State = RoomSceneState.Move;
	}
}