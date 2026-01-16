/*
#pragma strict
var gameName : String = "MIG35GameType";
var mainCamera : GameObject;

private var refresh : boolean;
private var hostData : HostData[]; //紀錄hostlist
private var connected : boolean = false;

private var roomButPos : Vector2 = Vector2(Screen.width * 0.7,Screen.height * 0.07);//activebut
private var connectPos : Vector2 = Vector2(Screen.width * 0.5,Screen.height * 0.07);//activebut

private var boxPos : Vector2 = Vector2(Screen.width * 0.7,Screen.height * 0.12);//按鈕box
private var boxPos2 : Vector2 = Vector2(Screen.width * 0.5,Screen.height * 0.10);//按鈕box2

private var activeButSize : Vector2 = Vector2(Screen.width/8.5, Screen.height/17);//玩家按鈕的大小
private var gate : int = 5;

function Awake (){
    
    if(Screen.width < 1250){
    	activeButSize.x = 105;//防止按鈕過小
    }
    
}

//Gui
function OnGUI (){
    var scriptA : O14_Music_Controller = GameObject.Find("Music Controller").GetComponent("O14_Music_Controller") as O14_Music_Controller;    
    
    if(!Network.isClient && !Network.isServer && connected == false){
	    GUI.Box (Rect(boxPos.x - (activeButSize.x + gate*2)/2,boxPos.y ,activeButSize.x + gate*2,((activeButSize.x + gate*2) * 0.20 )+ (gate * 3) + (activeButSize.y * 2)), "可加入的遊戲");
	    
	    if (GUI.Button (Rect (roomButPos.x - (activeButSize.x/2),boxPos.y + (gate * 1) + ((activeButSize.x + gate*2) * 0.20 ) + (activeButSize.y * 0),activeButSize.x,activeButSize.y), "建立遊戲")) {
		    scriptA.playGUI(0);//cor
		    startServer();//生成Server
		}
		if (GUI.Button (Rect (roomButPos.x - (activeButSize.x/2),boxPos.y + (gate * 2) + ((activeButSize.x + gate * 2) * 0.20 ) + (activeButSize.y * 1),activeButSize.x,activeButSize.y), "尋找遊戲")) {
		    refreshHostList();//重新取得清單
		    scriptA.playGUI(0);//cor
		}
		if(hostData){
			GUI.Box (Rect(boxPos2.x - (activeButSize.x + gate*2)/2,boxPos2.y ,activeButSize.x + gate*2,((activeButSize.x + gate * 2) * 0.20 )+ (gate * (hostData.length + 1)) + (activeButSize.y * (hostData.length))), "可加入的遊戲");
			
			
			for(var i:int = 0;i < hostData.length;i++){
			    if (GUI.Button (Rect (connectPos.x - (activeButSize.x/2),boxPos2.y + (gate * (i + 1)) + ((activeButSize.x + gate * 2) * 0.20 ) + (activeButSize.y * i),activeButSize.x ,activeButSize.y), hostData[i].gameName)) {
				    Network.Connect(hostData[i]);
				    //Debug.Log(hostData[i]);
				}
			}
		}
	}
}

//Server
function startServer(){
	var scriptA : O05_Data = GameObject.Find("Data").GetComponent("O05_Data") as O05_Data;
	Network.InitializeServer(32, 25001, !Network.HavePublicAddress);
	MasterServer.RegisterHost(gameName, scriptA.get_User() + "'s game", "We need more gold!");
	var scriptB : A05_Menu_Logic = mainCamera.GetComponent("A05_Menu_Logic") as A05_Menu_Logic;  
	scriptB.set_Text("遊戲已建立，請等待玩家加入...");  
}

function refreshHostList(){//請求List
	MasterServer.RequestHostList(gameName);
	refresh = true ;
	/*yield WaitForSeconds(5);//等待1.5秒
}

function Update() {
    if(refresh){
        if(MasterServer.PollHostList().Length > 0){
            refresh = false;
            hostData = MasterServer.PollHostList();//紀錄hostlist
        }
    }
}

function OnConnectedToServer(){
    Debug.Log("Server connected!");
    var scriptA : A03_SinglePlayer = mainCamera.GetComponent("A03_SinglePlayer") as A03_SinglePlayer;
    var scriptB : A06_RPC_Controller = GameObject.Find("World Controller").GetComponent("A06_RPC_Controller") as A06_RPC_Controller;

	scriptB.send_Name();//傳送姓名S
	scriptB.send_Data();//傳送姓名S
	
    connected = true;
    /*    
    scriptA.set_Player(1);//後攻設定L
    scriptB.send_Name();//傳送姓名S
    connected = true;
    
}

function OnPlayerConnected(){
    var scriptA : A03_SinglePlayer = mainCamera.GetComponent("A03_SinglePlayer") as A03_SinglePlayer;
    var scriptB : A06_RPC_Controller = GameObject.Find("World Controller").GetComponent("A06_RPC_Controller") as A06_RPC_Controller;        
    var order :int = Random.Range(0,2);
    
    scriptA.set_Player(order);//先攻設定L
    scriptB.send_Order(1 - order);//傳送姓名C
    
    
    
    scriptB.send_Name();//傳送姓名C
    connected = true;
    
    scriptA.board_Initialization();//初始化棋盤
    /*
    scriptA.set_Player(0);//先攻設定L
    scriptB.send_Name();//傳送姓名C
    connected = true;
    
}

//Debug.Log

function OnServerInitialized(){//Server生成
    Debug.Log("Server initialize!");
}

function OnMasterServerEvent(msE: MasterServerEvent) {//server事件
	if (msE == MasterServerEvent.RegistrationSucceeded) {
		Debug.Log("Server registered");
	}
}

//斷線設定

function OnFailedToConnect(){
    var scriptA : A05_Menu_Logic = mainCamera.GetComponent("A05_Menu_Logic") as A05_Menu_Logic;  
	scriptA.set_Text("另一位玩者逃離了遊戲..."); 
	scriptA.set_Win();
}

function OnPlayerDisconnected(){
    var scriptA : A05_Menu_Logic = mainCamera.GetComponent("A05_Menu_Logic") as A05_Menu_Logic;  
	scriptA.set_Text("另一位玩者逃離了遊戲..."); 
	scriptA.set_Win();
}
*/