package controllers;

import models.Client;
import models.Manager;
import vue.PanelClient;
import vue.PanelHistorique;
import models.*;
import vue.*;


import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

/**
 * Created by Pierre-Louis on 08/03/2017.
 */
public class ctrlHistorique {

    private Client client;
    private PanelHistorique panelHisto;
    private Manager manager = new Manager();


    public ctrlHistorique(Client client, PanelHistorique panelhistorique) {
        this.client = client;
        this.panelHisto = panelhistorique;
    }

    public void setAction(PanelClient panelclient)
    {
        PanelClient lepanelclient = panelclient;
        ArrayList<Compte> lescomptes = manager.getComptesFromClient(client.getCodeClient());
        for(Compte leCompte : lescomptes)
        {
            panelHisto.getComboCompte().addItem(leCompte.getNumero());
        }


        // Coder l'alimentation de la combo opération
        panelHisto.getComboCompte().addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
            }
        });

        // Coder l'alimentation de la combo historique
        panelHisto.getCombohistorique().addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
            }
        });


        panelHisto.getBtnRetour().addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panelHisto.setVisible(false);
                lepanelclient.setVisible(true);
                lepanelclient.repaint();
                panelclient.getComboComptes().setSelectedIndex(0);
            }
        });
    }
}
