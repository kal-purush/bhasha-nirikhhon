package viewmodel;

import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import model.GuestModel;
import model.HostModel;
import model.Model;

import java.util.Observable;
import java.util.Observer;

public class ViewModel extends Observable implements Observer {

    Model m;
    public StringProperty playerName, gameName;

    public ViewModel() {
        playerName = new SimpleStringProperty();
        gameName = new SimpleStringProperty();
    }

    public void setModel(Model m) {
        this.m = m;
        m.addObserver(this);
    }

    public void createGame(String[] fileNames) {
        HostModel hostModel = new HostModel();
        this.setModel(hostModel);
        hostModel.createGame(playerName.getValue(), fileNames);
    }

    public void connectToGame() {
        GuestModel guestModel = new GuestModel();
        this.setModel(guestModel);
        guestModel.join(playerName.getValue(), gameName.getValue());
    }

    @Override
    public void update(Observable o, Object arg) {

    }
}