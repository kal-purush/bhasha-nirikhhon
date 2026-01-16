using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Net;
using System.Net.Sockets;

public class Client {
    private int id = -1;
    private string username = "";

    private Tcp tcp;
    private IPEndPoint endPointUdp;
    private PlayerController player;

    public Client(int id) {
        this.id = id;
    }

    public void sendTcpData(Packet packet) {
        tcp.sendData(packet);
    }

    public Tcp getTcp() {
        return tcp;
    }

    public void setTcp(Tcp tcp) {
        this.tcp = tcp;
    }

    public int getId() {
        return id;
    }

    public IPEndPoint getEndPointUdp() {
        return endPointUdp;
    }

    public void setEndPointUdp(IPEndPoint endPointUdp) {
        this.endPointUdp = endPointUdp;
    }

    public string getUsername() {
        return username;
    }

    public void setUsername(string username) {
        this.username = username;
    }

    public void setPlayer(PlayerController player) {
        this.player = player;
    }

    public PlayerController getPlayer() {
        return player;
    }

    public void disconnect() {
        tcp.disconnect();
        tcp = null;
        endPointUdp = null;
    }
}