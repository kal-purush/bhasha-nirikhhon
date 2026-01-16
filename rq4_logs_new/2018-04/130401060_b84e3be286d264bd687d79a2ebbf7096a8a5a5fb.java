package ru.itis.javawarrior.service.impl;

import java.util.ArrayList;
import java.util.List;

import ru.itis.javawarrior.service.ActionService;
import ru.itis.javawarrior.util.ActionEnum;

/**
 * Используется в компилированном классе
 */
public class ActionServiceImpl implements ActionService {

    public List<ActionEnum> responseActions = new ArrayList<>();
    private int sequence = 0;

    @Override
    public void walk() {
        responseActions.add(ActionEnum.MOVE_FORWARD);

    }

    @Override
    public void shoot() {
        responseActions.add(ActionEnum.SHOOT);
    }

    @Override
    public void jump() {
        responseActions.add(ActionEnum.FLIP_FORWARD);
    }
}