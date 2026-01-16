using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Collections;
using System.Net;
using System.Web;
using System.Data;
using System.Threading;
using System.Timers;
using System.Diagnostics;
using System.Reflection;

using PRoCon.Core;
using PRoCon.Core.Plugin;
using PRoCon.Core.Plugin.Commands;
using PRoCon.Core.Players;
using PRoCon.Core.Players.Items;
using PRoCon.Core.Battlemap;
using PRoCon.Core.Maps;


namespace PRoConEvents
{

//Aliases
using EventType = PRoCon.Core.Events.EventType;
using CapturableEvent = PRoCon.Core.Events.CapturableEvents;

public class BFJC_MapManager : PRoConPluginAPI, IPRoConPluginInterface
{

/* Inherited:
    this.PunkbusterPlayerInfoList = new Dictionary<string, CPunkbusterInfo>();
    this.FrostbitePlayerInfoList = new Dictionary<string, CPlayerInfo>();
*/

private bool fIsEnabled;
private int fDebugLevel;

//private MaplistMode maplistMode = MaplistMode.NONE;
private int warmupModeBoundarySetting = 32;
private int fullModeBoundarySetting = 48;
private int warmupModeTicketPercent = 100;
private int fullModeTicketPercent = 100;
private bool isChangeServerNameSetting = false;
private string warmupModeServerNameSetting = String.Empty;
private string fullModeServerNameSetting = String.Empty;
private string[] warmupMaplistSetting;
private string[] fullMaplistSetting;
private List<MapInfo> warmupMapInfoList;
private List<MapInfo> fullMapInfoList;
//private List<MaplistEntry> serverMaplist = null;
private int playerCount = 0;

public bool IsWarmupMode(List<MaplistEntry> serverMaplist)
{
    if (serverMaplist != null && warmupMapInfoList != null)
    {
        if (serverMaplist.Count == warmupMapInfoList.Count)
        {
            return true;
        }
    }

    return false;
}


public bool IsFullMode(List<MaplistEntry> serverMaplist)
{
    if (serverMaplist != null && fullMapInfoList != null)
    {
        if (serverMaplist.Count == fullMapInfoList.Count)
        {
            return true;
        }
    }

    return false;
}

public BFJC_MapManager() {
	fIsEnabled = false;
	fDebugLevel = 2;
}

public enum MessageType { Warning, Error, Exception, Normal };

public String FormatMessage(String msg, MessageType type) {
	String prefix = "[^bBFJC-MapManager^n] ";

	if (type.Equals(MessageType.Warning))
		prefix += "^1^bWARNING^0^n: ";
	else if (type.Equals(MessageType.Error))
		prefix += "^1^bERROR^0^n: ";
	else if (type.Equals(MessageType.Exception))
		prefix += "^1^bEXCEPTION^0^n: ";

	return prefix + msg;
}


public void LogWrite(String msg)
{
	this.ExecuteCommand("procon.protected.pluginconsole.write", msg);
}

public void ConsoleWrite(string msg, MessageType type)
{
	LogWrite(FormatMessage(msg, type));
}

public void ConsoleWrite(string msg)
{
	ConsoleWrite(msg, MessageType.Normal);
}

public void ConsoleWarn(String msg)
{
	ConsoleWrite(msg, MessageType.Warning);
}

public void ConsoleError(String msg)
{
	ConsoleWrite(msg, MessageType.Error);
}

public void ConsoleException(String msg)
{
	ConsoleWrite(msg, MessageType.Exception);
}

public void DebugWrite(string msg, int level)
{
	if (fDebugLevel >= level) ConsoleWrite(msg, MessageType.Normal);
}


public void ServerCommand(params String[] args)
{
	List<string> list = new List<string>();
	list.Add("procon.protected.send");
	list.AddRange(args);
	this.ExecuteCommand(list.ToArray());
}


public string GetPluginName() {
    return "BFJC-MapManager";
}

public string GetPluginVersion() {
	return "0.0.4";
}

public string GetPluginAuthor() {
	return "Aogik";
}

public string GetPluginWebsite() {
    return "bf.jpcommunity.com/";
}

public string GetPluginDescription() {
	return @"
<h2>Description</h2>
<p>This Plugin control maplist. (Beta Version)</p>

<h2>Settings</h2>
<p>beta version</p>

<h2>Development</h2>
<p>Battlefield JP Community</p>

<h3>Changelog</h3>
<blockquote><h4>0.0.4 (2014/08/30)</h4>
	- Add Change Server Ticket Percent option<br/>
</blockquote>

<blockquote><h4>0.0.3 (2014/05/24)</h4>
	- Add Change Server Name option<br/>
</blockquote>

<blockquote><h4>0.0.2 (2014/04/13)</h4>
	- delete debug log<br/>
</blockquote>

<blockquote><h4>0.0.1 (2014/03/29)</h4>
	- initial version<br/>
</blockquote>
";
}




public List<CPluginVariable> GetDisplayPluginVariables() {

	List<CPluginVariable> lstReturn = new List<CPluginVariable>();

	lstReturn.Add(new CPluginVariable("BFJC-MapManager|Debug level", fDebugLevel.GetType(), fDebugLevel));

    lstReturn.Add(new CPluginVariable("Map Manage Settings|Warmup Mode Boundary", warmupModeBoundarySetting.GetType(), warmupModeBoundarySetting));
    lstReturn.Add(new CPluginVariable("Map Manage Settings|Full Mode Boundary", fullModeBoundarySetting.GetType(), fullModeBoundarySetting));

    lstReturn.Add(new CPluginVariable("Map Manage Settings|Warmup Mode Ticket Percent", warmupModeTicketPercent.GetType(), warmupModeTicketPercent));
    lstReturn.Add(new CPluginVariable("Map Manage Settings|Full Mode Ticket Percent", fullModeTicketPercent.GetType(), fullModeTicketPercent));

    lstReturn.Add(new CPluginVariable("Map Manage Settings|IsChangeServerName", isChangeServerNameSetting.GetType(), isChangeServerNameSetting));
    lstReturn.Add(new CPluginVariable("Map Manage Settings|Warmup Mode ServerName", warmupModeServerNameSetting.GetType(), warmupModeServerNameSetting));
    lstReturn.Add(new CPluginVariable("Map Manage Settings|Full Mode ServerName", fullModeServerNameSetting.GetType(), fullModeServerNameSetting));

    lstReturn.Add(new CPluginVariable("Map Manage Settings|Warmup Mode Maplist", typeof(string[]), warmupMaplistSetting));
    lstReturn.Add(new CPluginVariable("Map Manage Settings|Full Mode Maplist", typeof(string[]), fullMaplistSetting));

	return lstReturn;
}

public List<CPluginVariable> GetPluginVariables() {
	return GetDisplayPluginVariables();
}

public void SetPluginVariable(string strVariable, string strValue) {
	if (Regex.Match(strVariable, @"Debug level").Success) {
		int tmp = 2;
		int.TryParse(strValue, out tmp);
		fDebugLevel = tmp;
	}

    if (Regex.Match(strVariable, @"Warmup Mode Boundary").Success)
    {
        int tmp = 32;
        int.TryParse(strValue, out tmp);
        warmupModeBoundarySetting = tmp;

        ConsoleWrite("(Settings) Warmup Mode Boundary = " + warmupModeBoundarySetting);
    }

    if (Regex.Match(strVariable, @"Full Mode Boundary").Success)
    {
        int tmp = 48;
        int.TryParse(strValue, out tmp);
        fullModeBoundarySetting = tmp;

        ConsoleWrite("(Settings) Full Mode Boundary = " + fullModeBoundarySetting);
    }

    if (Regex.Match(strVariable, @"Warmup Mode Ticket Percent").Success)
    {
        int tmp = 100;
        int.TryParse(strValue, out tmp);
        warmupModeTicketPercent = tmp;

        ConsoleWrite("(Settings) Warmup Mode Ticket Percent = " + warmupModeTicketPercent);
    }

    if (Regex.Match(strVariable, @"Full Mode Ticket Percent").Success)
    {
        int tmp = 100;
        int.TryParse(strValue, out tmp);
        fullModeTicketPercent = tmp;

        ConsoleWrite("(Settings) Full Mode Ticket Percent = " + fullModeTicketPercent);
    }

    if (Regex.Match(strVariable, @"IsChangeServerName").Success)
    {
        bool tmp = false;
        Boolean.TryParse(strValue, out tmp);
        isChangeServerNameSetting = tmp;

        ConsoleWrite("(Settings) IsChangeServerName = " + isChangeServerNameSetting);
    }

    if (Regex.Match(strVariable, @"Warmup Mode ServerName").Success)
    {
        warmupModeServerNameSetting = strValue;

        ConsoleWrite("(Settings) Warmup Mode ServerName = " + warmupModeServerNameSetting);
    }

    if (Regex.Match(strVariable, @"Full Mode ServerName").Success)
    {
        fullModeServerNameSetting = strValue;

        ConsoleWrite("(Settings) Full Mode ServerName = " + fullModeServerNameSetting);
    }


    if (Regex.Match(strVariable, @"Warmup Mode Maplist").Success)
    {
        this.warmupMaplistSetting = CPluginVariable.DecodeStringArray(strValue);
        this.LoadMapList();

        ConsoleWrite("(Settings) Warmup Mode Maplist Count = " + this.warmupMaplistSetting.Length);
    }

    if (Regex.Match(strVariable, @"Full Mode Maplist").Success)
    {
        this.fullMaplistSetting = CPluginVariable.DecodeStringArray(strValue);
        this.LoadMapList();

        ConsoleWrite("(Settings) Full Mode Maplist Count = " + this.fullMaplistSetting.Length);
    }
}


public void OnPluginLoaded(string strHostName, string strPort, string strPRoConVersion) {
	//this.RegisterEvents(this.GetType().Name, "OnVersion", "OnServerInfo", "OnResponseError", "OnListPlayers", "OnPlayerJoin", "OnPlayerLeft", "OnPlayerKilled", "OnPlayerSpawned", "OnPlayerTeamChange", "OnGlobalChat", "OnTeamChat", "OnSquadChat", "OnRoundOverPlayers", "OnRoundOver", "OnRoundOverTeamScores", "OnLoadingLevel", "OnLevelStarted", "OnLevelLoaded");
    this.RegisterEvents(this.GetType().Name, "OnVersion", "OnServerInfo", "OnResponseError", "OnListPlayers", "OnPlayerJoin", "OnPlayerLeft", "OnPlayerKilled", "OnPlayerSpawned", "OnPlayerTeamChange", "OnGlobalChat", "OnTeamChat", "OnSquadChat", "OnRoundOverPlayers", "OnRoundOver", "OnRoundOverTeamScores", "OnLoadingLevel", "OnLevelStarted", "OnLevelLoaded", "OnMaplistList");
}

public void OnPluginEnable() {
    // load map list from settings
    LoadMapList();

    //this.maplistMode = MaplistMode.NONE;
    this.fIsEnabled = true;
	ConsoleWrite("Enabled!");
}

public void OnPluginDisable() {
    this.fIsEnabled = false;
	ConsoleWrite("Disabled!");
}


public override void OnVersion(string serverType, string version) { }

public override void OnServerInfo(CServerInfo serverInfo) {
	//ConsoleWrite("Debug level = " + fDebugLevel);

    if (this.isChangeServerNameSetting)
    {
        // ������T�[�o���ݒ肪��̏ꍇ�́A�f�t�H���g�̖��O�ɂ���
        if (String.IsNullOrEmpty(this.warmupModeServerNameSetting))
        {
            this.warmupModeServerNameSetting = serverInfo.ServerName;
        }
        if (String.IsNullOrEmpty(this.fullModeServerNameSetting))
        {
            this.fullModeServerNameSetting = serverInfo.ServerName;
        }

        /*
         * �T�[�o���̕ύX 
         */
        if (serverInfo.PlayerCount < this.warmupModeBoundarySetting)
        {
            if (serverInfo.ServerName != this.warmupModeServerNameSetting)
            {
                // �E�H�[�~���O�A�b�v���[�h���̃T�[�o���ɕύX
                SetServerName(this.warmupModeServerNameSetting);
            }
        }
        else if (serverInfo.PlayerCount > this.fullModeBoundarySetting)
        {
            if (serverInfo.ServerName != this.fullModeServerNameSetting)
            {
                // �t�����[�h���̃T�[�o���ɕύX
                SetServerName(this.fullModeServerNameSetting);
            }
        }
    }
}

public override void OnResponseError(List<string> requestWords, string error) { }

public override void OnListPlayers(List<CPlayerInfo> players, CPlayerSubset subset)
{
    //ConsoleWrite("OnListPlayers: " + players.Count + " (WARMUP MODE <= " + this.warmupModeBoundarySetting + " / " + this.fullModeBoundarySetting + " <= FULL MODE)");

    // ���݂̃v���C���[����ێ�
    playerCount = players.Count;

    // ���݂̃}�b�v���X�g�����N�G�X�g
    RequestMaplist();
}

public override void OnMaplistList(List<MaplistEntry> lstMaplist)
{
    //ConsoleWrite("OnMaplistList:" + lstMaplist.Count);

    if (this.playerCount <= this.warmupModeBoundarySetting && !this.IsWarmupMode(lstMaplist))
    {
        ConsoleWrite(">>>>>>> Change To WARMUP Mode Maplist !!!");

        /*
         * �E�H�[���A�b�v�p�}�b�v���X�g�ɕύX
         */
        ChangeMaplist(this.warmupMapInfoList);

        /*
         * �E�H�[���A�b�v�p�Ƀ`�P�b�g����ύX
         */
        SetServerTicketPercent(this.warmupModeTicketPercent); 

        //// ���݂̃}�b�v���X�g�ێ�
        //this.maplistMode = MaplistMode.WARMUP;

        // ADMIN���b�Z�[�W
        SendGlobalMessage("Change to WARMUP MODE MapList !!");
    }

    if (this.playerCount >= this.fullModeBoundarySetting && !this.IsFullMode(lstMaplist))
    {
        ConsoleWrite(">>>>>>> Change To FULL Mode Maplist !!!");

        /*
         * �t���p�}�b�v���X�g�ɕύX
         */
        ChangeMaplist(this.fullMapInfoList);

        /*
         * �t���p�Ƀ`�P�b�g����ύX
         */
        SetServerTicketPercent(this.fullModeTicketPercent); 

        //// ���݂̃}�b�v���X�g�ێ�
        //this.maplistMode = MaplistMode.FULL;

        // ADMIN���b�Z�[�W
        SendGlobalMessage("Change to FULL MODE MapList !!");
    }

    //this.serverMaplist = lstMaplist;
}


public override void OnPlayerJoin(string soldierName) {
}

public override void OnPlayerLeft(CPlayerInfo playerInfo) {
}

public override void OnPlayerKilled(Kill kKillerVictimDetails) { }

public override void OnPlayerSpawned(string soldierName, Inventory spawnedInventory) { }

public override void OnPlayerTeamChange(string soldierName, int teamId, int squadId) { }

public override void OnGlobalChat(string speaker, string message) { }

public override void OnTeamChat(string speaker, string message, int teamId) { }

public override void OnSquadChat(string speaker, string message, int teamId, int squadId) { }

public override void OnRoundOverPlayers(List<CPlayerInfo> players) { }

public override void OnRoundOverTeamScores(List<TeamScore> teamScores) { }

public override void OnRoundOver(int winningTeamId) { }

public override void OnLoadingLevel(string mapFileName, int roundsPlayed, int roundsTotal) { }

public override void OnLevelStarted() { }

public override void OnLevelLoaded(string mapFileName, string Gamemode, int roundsPlayed, int roundsTotal) { } // BF3

private void LoadMapList()
{
    this.warmupMapInfoList = new List<MapInfo>();
    for (int i = 0; i < this.warmupMaplistSetting.Length; i++)
    {
        MapInfo mapInfo = new MapInfo(this.warmupMaplistSetting[i], i);

        this.warmupMapInfoList.Add(mapInfo);
    }

    this.fullMapInfoList = new List<MapInfo>();
    for (int i = 0; i < this.fullMaplistSetting.Length; i++)
    {
        MapInfo mapInfo = new MapInfo(this.fullMaplistSetting[i], i);

        this.fullMapInfoList.Add(mapInfo);
    }
}

private void RequestMaplist()
{
    // Request current maplist
    string startIndex = "0";
    this.ExecuteCommand("procon.protected.send", "mapList.list", startIndex);
}

private void ChangeMaplist(IEnumerable mapInfoList)
{
    ConsoleWrite("START Changing Maplist:");
    foreach (MapInfo mapInfo in mapInfoList)
    {
        ConsoleWrite(mapInfo.ToString());
    }
    
    // Clear map list
    this.ExecuteCommand("procon.protected.send", "mapList.clear");

    // Add map list
    foreach (MapInfo mapInfo in mapInfoList)
    {
        this.ExecuteCommand("procon.protected.send", "mapList.add", mapInfo.MapFileName, mapInfo.Gamemode, mapInfo.Rounds.ToString(), mapInfo.Index.ToString());
    }

    // Set map index
    string nextMapIndex = "0";
    this.ExecuteCommand("procon.protected.send", "mapList.setNextMapIndex", nextMapIndex);

    //// Run next round
    //if (this.m_iCurrentPlayerCount == 0 && (!this.m_blRoundEnded || this.m_blRoundEnded && this.m_blRestartRequested) || this.m_enRestartNow == enumBoolYesNo.Yes && this.m_iCurrentPlayerCount <= this.m_iRestartLimit && !this.m_blRoundEnded)
    //{
    //    this.ExecuteCommand("procon.protected.send", "mapList.runNextRound");
    //}
    //this.m_blMapListChanged = false;

    ConsoleWrite("DONE");
}

/// <summary>
/// SET SERVER NAME
/// </summary>
/// <param name="newServerName"></param>
private void SetServerName(string newServerName)
{
    this.ExecuteCommand("procon.protected.send", "vars.serverName", newServerName);
    ConsoleWrite("=== Server Name set to: '" + newServerName + "'");
}

/// <summary>
/// SET SERVER GameModeCounter
/// </summary>
/// <param name="newServerName"></param>
private void SetServerTicketPercent(int ticketPercent)
{
    this.ExecuteCommand("procon.protected.send", "vars.gameModeCounter", ticketPercent.ToString());
    ConsoleWrite("=== Server Ticket Percent set to: " + ticketPercent + "%");
}

/// <summary>
/// ADMIN SAY
/// </summary>
/// <param name="message"></param>
private void SendGlobalMessage(String message)
{
    string pluginName = "(" + GetPluginName() + ") ";
    ServerCommand("admin.say", pluginName + message, "all");
}

} // end BFJC_MapManager

/// <summary>
/// �}�b�v�C���t�H
/// </summary>
public class MapInfo
{
    public string MapFileName { get; set; }

    public string Gamemode { get; set; }

    public int Rounds { get; set; }

    public int Index { get; set; }

    public MapInfo(string mapInfoString, int index)
    {
        string[] mapInfo = mapInfoString.Split(' ');
        if (mapInfo.Length != 3)
        {
            throw new Exception("mapInfoString is invalid format");
        }

        MapFileName = mapInfo[0];
        Gamemode = mapInfo[1];
        Rounds = 1;

        int round;
        if (int.TryParse(mapInfo[2], out round))
        {
            Rounds = round;
        }
        
        Index = index;
    }

    public override string ToString()
    {
        return MapFileName + " " + Gamemode + " " + Rounds + " [" + Index + "]";
    }
}

/// <summary>
/// �K�p���̃}�b�v���X�g�̏��
/// </summary>
public enum MaplistMode
{
    NONE,
    WARMUP,
    FULL
}

} // end namespace PRoConEvents


