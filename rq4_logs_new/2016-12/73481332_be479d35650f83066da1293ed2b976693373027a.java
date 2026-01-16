package networkMarket.client.controllers;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import networkMarket.interfaces.MarketPlace;
import networkMarket.marketPlace.User;


/**
 * Created by daseel on 12/6/16.
 */
public class MyItemsController implements Controller {

    private User user;
    private MarketPlace market;


    @FXML
    Button backButton;

    @Override
    public void init(User user, MarketPlace market) {
        this.user = user;
        this.market = market;
    }


}