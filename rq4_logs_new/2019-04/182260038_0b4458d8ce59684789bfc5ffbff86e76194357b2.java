package fr.eni.javaee.bll;

import java.util.ArrayList;
import java.util.List;

import fr.eni.javaee.bo.Client;

public class ClientManager {
	List<Client> clients = new ArrayList<>();
	private static ClientManager instance = null;

	public List<Client> getClients() {
		return clients;
	}
	
	public Client getClientById(int id) {
		return clients.stream().filter(clientC -> clientC.getId() == id).findAny().orElse(null);
	}
	
	public Client getClientByName(String name) {
		return clients.stream().filter(clientC -> clientC.getName().equalsIgnoreCase(name)).findAny().orElse(null);
	}
	
	public void addClient(Client client) {
		this.clients.add(client);
	}
	
	public void deleteClient(int id) {
		Client client = this.getClientById(id);
		this.clients.remove(client);
	}

	public static synchronized ClientManager getInstance() {
		if(instance == null) {
			instance = new ClientManager();
		}
		
		return instance;
	}
}