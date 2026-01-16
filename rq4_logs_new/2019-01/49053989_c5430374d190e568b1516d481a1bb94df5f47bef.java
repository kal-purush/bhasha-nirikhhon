package org.aiwolf.ui.net;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aiwolf.common.AIWolfAgentException;
import org.aiwolf.common.AIWolfRuntimeException;
import org.aiwolf.common.data.Agent;
import org.aiwolf.common.data.Player;
import org.aiwolf.common.data.Request;
import org.aiwolf.common.data.Role;
import org.aiwolf.common.net.DataConverter;
import org.aiwolf.common.net.GameSetting;
import org.aiwolf.common.net.Packet;
import org.aiwolf.common.net.TalkToSend;
import org.aiwolf.common.util.BidiMap;
import org.aiwolf.server.IllegalPlayerNumException;
import org.aiwolf.server.LostClientException;
import org.aiwolf.server.net.ServerListener;
import org.aiwolf.server.net.TcpipServer;
import org.aiwolf.ui.AgentPanel;

/**
 * Use both direct and tcpip for connection.
 * @author xtori
 *
 */
public class TcpipDirectServer extends TcpipServer {

	BidiMap<Player, Agent> playerAgentMap;
	Map<Agent, Role> requestRoleMap;
	List<Agent> agentList = new ArrayList<>();

	public TcpipDirectServer(int port, int limit, GameSetting gameSetting) {
		super(port, limit, gameSetting);
		playerAgentMap = new BidiMap<>();
		requestRoleMap = new HashMap<>();
	}

	/**
	 * DirectConnected Player
	 * @param player
	 * @param role
	 */
	public void add(String name, Player player, Role role) {
		if(playerAgentMap.size() >= limit) {
			throw new AIWolfRuntimeException("Too many players added");
		}

		int idx = playerAgentMap.size()+1;
		Agent agent = Agent.getAgent(idx);
		playerAgentMap.put(player, agent);
		nameMap.put(agent, name);
		requestRoleMap.put(agent, null);
		System.out.println("add "+name);

	}

	@Override
	public void waitForConnection() throws IOException, SocketTimeoutException{


	    for(Socket sock:socketAgentMap.keySet()){
	    	if(sock != null && sock.isConnected()){
	    		sock.close();
	    	}
	    }

	    socketAgentMap.clear();

//		serverLogger.info(String.format("Waiting for connection...\n"));
	    System.out.println("Waiting for connection...\n");

	    serverSocket = new ServerSocket(port);

	    isWaitForClient = true;


	    int startIdx = playerAgentMap.size()+socketAgentMap.size()+1;
	    System.out.println(socketAgentMap.size()+":"+playerAgentMap.size());
	    while(socketAgentMap.size()+playerAgentMap.size() < limit && isWaitForClient){
	        Socket socket = serverSocket.accept();

	        synchronized (socketAgentMap) {
		        Agent agent = null;
				for (int i = startIdx; i <= limit; i++) {
		        	if(!socketAgentMap.containsValue(Agent.getAgent(i)) && !playerAgentMap.containsValue(agent)){
		        		agent = Agent.getAgent(i);
		        		break;
		        	}
		        }
		        if(agent == null){
		        	throw new IllegalPlayerNumException("Fail to create agent");
		        }
				socketAgentMap.put(socket, agent);
				String name = requestName(agent);
				nameMap.put(agent, name);

				for(ServerListener listener:serverListenerSet){
					listener.connected(socket, agent, name);
				}
	        }

	    }
	    isWaitForClient = false;
	    serverSocket.close();
	}


	@Override
	public List<Agent> getConnectedAgentList() {
		if(agentList == null) {
			agentList = new ArrayList<>();
		}
		synchronized (agentList) {
			if(isWaitForClient() || agentList.size() != limit) {
				agentList.clear();
				agentList.addAll(socketAgentMap.values());
				agentList.addAll(playerAgentMap.values());
			}
		}
		return new ArrayList<>(agentList);
	}
















	@Override
	public String requestName(Agent agent) {
		if(nameMap.containsKey(agent)){
			return nameMap.get(agent);
		}
		if(socketAgentMap.containsValue(agent)) {
			return super.requestName(agent);
		}
		else {
			try{
				String name = playerAgentMap.getKey(agent).getName();
				if(name != null){
					return name;
				}
				else{
					return playerAgentMap.getKey(agent).getClass().getSimpleName();
				}
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestName", e);
			}
		}
	}



	@Override
	public Role requestRequestRole(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestRequestRole(agent);
		}
		else {
			try{
				return requestRoleMap.get(agent);
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestRequestRole", e);
			}
		}
	}

	@Override
	public void init(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			super.init(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).initialize(gameData.getGameInfo(agent), gameSetting.clone());
			}catch(Throwable e){
				e.printStackTrace();
				throw new AIWolfAgentException(agent, "init", e);
			}
		}
	}

	@Override
	public void dayStart(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			super.dayStart(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				playerAgentMap.getKey(agent).dayStart();
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "dayStart", e);
			}
		}
	}

	@Override
	public void dayFinish(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			super.dayFinish(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
		//		playerAgentMap.getKey(agent).dayStart();
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "dayFinish", e);
			}
		}
	}

	@Override
	public String requestTalk(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestTalk(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				String talk = playerAgentMap.getKey(agent).talk();
				return talk;
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestTalk", e);
			}
		}
	}

	@Override
	public String requestWhisper(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestWhisper(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				String whisper = playerAgentMap.getKey(agent).whisper();
				return whisper;
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestWhisper", e);
			}
		}
	}

	@Override
	public Agent requestVote(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestVote(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				Agent target = playerAgentMap.getKey(agent).vote();
				return target;
		//		if(target == null){
		//			throw new NoReturnObjectException();
		//		}
		//		else{
		//			return target;
		//		}
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestVote", e);
			}
		}
	}

	@Override
	public Agent requestDivineTarget(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestDivineTarget(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				Agent target = playerAgentMap.getKey(agent).divine();
				return target;

			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestDivineTarget", e);
			}
		}
	}

	@Override
	public Agent requestGuardTarget(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestGuardTarget(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				Agent target = playerAgentMap.getKey(agent).guard();
				return target;
		//		if(target == null){
		//			throw new NoReturnObjectException();
		//		}
		//		else{
		//			return target;
		//		}
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestGuardTarget", e);
			}
		}
	}

	@Override
	public Agent requestAttackTarget(Agent agent) {
		if(socketAgentMap.containsValue(agent)) {
			return super.requestAttackTarget(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getGameInfo(agent));
				Agent target = playerAgentMap.getKey(agent).attack();
				return target;

			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "requestAttackTarget", e);
			}
		}
	}

	@Override
	public void finish(Agent agent){
		if(socketAgentMap.containsValue(agent)) {
			super.finish(agent);
		}
		else {
			try{
				playerAgentMap.getKey(agent).update(gameData.getFinalGameInfo(agent));
				playerAgentMap.getKey(agent).finish();
			}catch(Throwable e){
				throw new AIWolfAgentException(agent, "finish", e);
			}
		}
	}



}