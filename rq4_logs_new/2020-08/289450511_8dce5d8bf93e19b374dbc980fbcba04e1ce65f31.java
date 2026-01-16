package controller;

import dao.PassengerDAO;
import model.Passenger;

public class PassengerController {

    PassengerDAO passengerDAO = new PassengerDAO();

    public void  save(Passenger passenger){
        passengerDAO.saveEntity(passenger);
    }

    public void  update(Passenger passenger){
        passengerDAO.updateEntity(passenger);
    }

    public void  remove(Passenger passenger){
        passengerDAO.removeEntity(passenger);
    }


    public Passenger getBId(Long id){
        return passengerDAO.getEntityById(id);
    }


}