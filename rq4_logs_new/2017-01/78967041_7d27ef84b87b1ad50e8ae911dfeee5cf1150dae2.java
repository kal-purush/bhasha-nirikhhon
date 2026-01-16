package fr.uvsq.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import fr.uvsq.DAO.interfaces.DAO;
import fr.uvsq.DAO.interfaces.DAOAdresse;
import fr.uvsq.bean.Adresse;



@Service("adresseService")
public class AdresseService {
	
	@Autowired
    DAOAdresse daoAdresse;
	
	public void setDaoAdresse(DAOAdresse daoAdresse) {
		this.daoAdresse = daoAdresse;
	}

	
	public Adresse findById(long id) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public boolean insert(Adresse nouveau) {
		daoAdresse.insert(nouveau);
		return false;
	}

	
	public boolean update(Adresse nouveau) {
		// TODO Auto-generated method stub
		return false;
	}

	
}