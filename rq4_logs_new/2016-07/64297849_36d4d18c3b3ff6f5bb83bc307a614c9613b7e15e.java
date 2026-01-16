package com.swapnil.leveleditor.util.forms;

import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.graphics.g2d.TextureAtlas;
import com.badlogic.gdx.scenes.scene2d.Actor;
import com.badlogic.gdx.scenes.scene2d.ui.*;
import com.badlogic.gdx.scenes.scene2d.utils.ChangeListener;
import com.swapnil.leveleditor.item.Camera;

public class CameraForm {

    Table specMenu;
    TextField deviationAngleField;
    TextField widthField, heightField;

    public CameraForm(Camera camera) {
        TextureAtlas atlas = new TextureAtlas(Gdx.files.internal("UI/atlas.pack"));
        Skin skin = new Skin(Gdx.files.internal("Skins/MenuSkin.json"), atlas);

        TextButton rotate = new TextButton("Check Movement", skin);
        rotate.addListener(new ChangeListener() {
            @Override
            public void changed(ChangeEvent event, Actor actor) {
                camera.setRotate(!camera.isRotate());
            }
        });
        specMenu = new Table();

        List<String> list = new List<>(skin);
        list.setItems("Width", "Height", "D.Angle");

        char[] accepted = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '-'};
        TextField.TextFieldFilter textFieldFilter = (textField, c) -> {
            for (char a : accepted)
                if (a == c) return true;

            return false;
        };

        heightField = new TextField("", skin);
        widthField = new TextField("", skin);
        deviationAngleField = new TextField("", skin);

        heightField.setTextFieldFilter(textFieldFilter);
        widthField.setTextFieldFilter(textFieldFilter);
        deviationAngleField.setTextFieldFilter(textFieldFilter);

        widthField.addListener(new ChangeListener() {
            @Override
            public void changed(ChangeEvent event, Actor actor) {
                list.setSelectedIndex(0);
                try {
                    float widthParsed = Float.parseFloat(widthField.getText());
                    camera.setWidth(widthParsed);
                } catch (NumberFormatException e) {
                    //DO NOTHING
                }

            }
        });
        heightField.addListener(new ChangeListener() {
            @Override
            public void changed(ChangeEvent event, Actor actor) {
                list.setSelectedIndex(1);
                try {
                    float heightParsed = Float.parseFloat(heightField.getText());
                    camera.setHeight(heightParsed);
                } catch (NumberFormatException e) {
                    //DO NOTHING
                }
            }
        });
        deviationAngleField.addListener(new ChangeListener() {
            @Override
            public void changed(ChangeEvent event, Actor actor) {
                list.setSelectedIndex(2);
                try {
                    float devAngleParsed = Float.parseFloat(deviationAngleField.getText());
                    camera.setDeviationAngle(devAngleParsed);
                }catch (NumberFormatException e) {
                    //DO NOTHING
                }
            }
        });

        Table textFieldTable = new Table();
        textFieldTable.add(widthField).row();
        textFieldTable.add(heightField).row();
        textFieldTable.add(deviationAngleField).row();
        textFieldTable.pack();

        specMenu.add(list);
        specMenu.add(textFieldTable).row();
        specMenu.add(rotate).colspan(2);
        specMenu.pack();
    }
    public void setDefaults(float width, float height, float angle) {
        widthField.setText(Integer.toString((int)width));
        heightField.setText(Integer.toString((int)height));
        deviationAngleField.setText(Float.toString(angle));
    }

    public Table getForm() {
        return specMenu;
    }

}