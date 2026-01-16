package common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDragonController extends Remote {
	public void sendMessage(Message msg) throws RemoteException;

	public void receiveMessage(Message msg) throws RemoteException;

	public String getMetrics() throws RemoteException;

}