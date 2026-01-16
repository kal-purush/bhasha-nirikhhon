package ru.pfur.skis.ui;

import javafx.geometry.Point3D;
import ru.pfur.skis.Service;
import ru.pfur.skis.command.CopyNodesCommand;
import ru.pfur.skis.command.TranslateNodesCommand;
import ru.pfur.skis.model.Bar;
import ru.pfur.skis.model.Model;
import ru.pfur.skis.model.Node;
import ru.pfur.skis.observer.ChangeElementSubscriber;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.util.stream.Collectors;

/**
 * Created by Kamran on 5/7/2016.
 */
public class CopyTranslateWindow extends JFrame implements FocusListener, ChangeElementSubscriber {
    public static final int WIDTH = 300;
    public static final int HEIGHT = 230;

    JPanel panelForX;
    JPanel panelForY;
    JPanel panelForZ;
    JPanel panelForButtons;

    private JLabel labelX;
    private JLabel labelY;
    private JLabel labelZ;
    private Model model;

    private JTextField textFieldGetX;
    private JTextField textFieldGetY;
    private JTextField textFieldGetZ;

    private JButton copyTranslate;
    private JButton buttonClose;
    private Node node;
    private WindowState state;

    public CopyTranslateWindow(String name, Model model) {
        super(name);
        this.model = model;
        init();
        this.state = WindowState.EDIT;
    }

    public void init() {
        setTitle("Translate and Copy");
        setVisible(false);
        setAlwaysOnTop(true);
        Service.center(this, WIDTH, HEIGHT);
        setSize(new Dimension(WIDTH, HEIGHT));
        setLayout(new FlowLayout());
        setResizable(false);

        panelForX = new JPanel();
        labelX = new JLabel("X  =");
        labelX.setPreferredSize(new Dimension(25, 30));
        panelForX.add(labelX);
        textFieldGetX = new JTextField("");
        textFieldGetX.setPreferredSize(new Dimension(170, 30));
        panelForX.add(textFieldGetX);

        panelForY = new JPanel();
        labelY = new JLabel("Y  = ");
        labelY.setPreferredSize(new Dimension(25, 30));
        panelForY.add(labelY);
        textFieldGetY = new JTextField("");
        textFieldGetY.setPreferredSize(new Dimension(170, 30));
        panelForY.add(textFieldGetY);

        panelForZ = new JPanel();
        labelZ = new JLabel("Z  =");
        labelZ.setPreferredSize(new Dimension(25, 30));
        panelForZ.add(labelZ);
        textFieldGetZ = new JTextField("");
        textFieldGetZ.setPreferredSize(new Dimension(170, 30));
        panelForZ.add(textFieldGetZ);


        panelForButtons = new JPanel();
        copyTranslate = new JButton("Translate");
        copyTranslate.setPreferredSize(new Dimension(100, 30));

        copyTranslate.addActionListener(e -> {
            doAction();
            hideThis();

        });

        buttonClose = new JButton("Close");
        buttonClose.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                hideThis();
            }
        });
        buttonClose.setPreferredSize(new Dimension(80, 30));

        panelForButtons.add(copyTranslate);
        panelForButtons.add(buttonClose);

        this.add(panelForX);
        this.add(panelForY);
        this.add(panelForZ);
        this.add(panelForButtons);
    }

    private void doAction() {
        int x = Integer.parseInt(textFieldGetX.getText());
        int y = Integer.parseInt(textFieldGetY.getText());
        int z = Integer.parseInt(textFieldGetZ.getText());

        java.util.List<Node> selected = model.getNodes().stream().filter(n -> n.isSelected()).collect(Collectors.toList());
        new CopyNodesCommand(model, selected);
        selected = model.getNodes().stream().filter(n -> n.isSelected()).collect(Collectors.toList());
        new TranslateNodesCommand(model, selected, new Point3D(x, y, z));
    }

    private void hideThis() {
        this.setVisible(false);
    }

    @Override
    public void focusGained(FocusEvent e) {

    }

    @Override
    public void focusLost(FocusEvent e) {

    }

    @Override
    public void nodeSelectedChanged(Model model, Node node) {
        this.node = node;
    }

    @Override
    public void barSelectedChanged(Model model, Bar bar) {

    }

    @Override
    public void nodeTranslateChanged(Model model, Node node) {

    }

    @Override
    public void barNodeChanged(Model model, Bar bar) {

    }
}