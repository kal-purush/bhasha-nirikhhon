/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tictactoe.domain.model;

import javafx.scene.control.Button;

/**
 *
 * @author medos
 */
public class Tile {

    Button btn;
    Integer position;

    public Tile(int position) {
        this.btn = new Button();
        this.position = position;
    }

    public Button getBtn() {
        return btn;
    }

    public Integer getPosition() {
        return position;
    }
}