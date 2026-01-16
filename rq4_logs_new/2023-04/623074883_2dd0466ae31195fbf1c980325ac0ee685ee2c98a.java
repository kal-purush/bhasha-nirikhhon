package com.groupthree.mancala;

import com.groupthree.mancala.models.Player;
import com.groupthree.mancala.models.PublicInfo;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;

public abstract class ProfileCell extends ListCell<PublicInfo> {
    private final Label name;
    private final HBox hBox;
    private PublicInfo activeProfile;

    public ProfileCell(Player player) {
        super();
        Button removeButton = new Button("Remove");
        removeButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                PublicInfo info = getItem();
                player.removeFromFavorites(info);
                System.out.println("Action: " + getItem());
                updateList();
            }
        });
        name = new Label();
        Region region = new Region();
        HBox.setHgrow(region, Priority.ALWAYS);
        hBox = new HBox(name, region, removeButton);
        setText(null);
    }
    @Override
    public void updateItem(PublicInfo item, boolean empty) {
        super.updateItem(item, empty);
        setEditable(false);
        if (item != null) {
            name.setText(item.username());
            setGraphic(hBox);
        } else {
            setGraphic(null);
        }
    }
    public abstract void updateList();
}