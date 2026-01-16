/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package App;

import ActivitiesDataLayer.entities.Activities;
import ActivitiesDataLayer.entities.Voyageurs;
import ProtocolFUCAMP.GetListActRequest;
import ProtocolFUCAMP.GetListActResponse;
import ProtocolFUCAMP.GetListClientRequest;
import ProtocolFUCAMP.GetListClientResponse;
import ProtocolFUCAMP.GetListPartRequest;
import ProtocolFUCAMP.GetListPartResponse;
import ProtocolFUCAMP.RegisterRequest;
import ProtocolFUCAMP.RegisterResponse;
import ProtocolFUCAMP.TimeOut;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.collections.ObservableList;
import javafx.scene.control.ComboBox;
import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;

/**
 *
 * @author Arkios
 */
public class Client_Activities extends javax.swing.JDialog {
    Activities selectActivities;
    LinkedList<Voyageurs> listVoyInscrit;
    InetSocketAddress address;
    /**
     * Creates new form Client_Activities
     */
    public Client_Activities(java.awt.Frame parent, boolean modal, InetSocketAddress addr, Activities act) throws IOException, ClassNotFoundException {
        super(parent, modal);
        initComponents();
        selectActivities = act;
        address = addr;
        
        refreshActivitiesTable();
        refreshVoyageursTable();
    }
    
    private void refreshActivitiesTable() throws IOException, ClassNotFoundException{
        Activities tmp = null;
        Socket s = new Socket();
        s.connect(address);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        //Envoie Req
        oos.writeObject(new GetListActRequest());
        oos.flush();
        
        
        ObjectInputStream ios = new ObjectInputStream(s.getInputStream());
        //Recep Rep
        Object rep = null;
        rep = ios.readObject();
        
        if(rep instanceof TimeOut){
            System.out.println("To Do Timeout refreshActivities");
            return;
        }
        
        
        if(!(rep instanceof GetListActResponse)){
            System.out.println("To Do Other things: " + rep.getClass());
            return;
        }
        
        switch(((GetListActResponse) rep).getCode()){
            case GetListActResponse.SUCCESS: 
                    LinkedList<Activities> listact = ((GetListActResponse) rep).getList();
                    
                    //Recherche List
                    for(Activities a : listact){
                        if(a.getIdActivite().intValue() == selectActivities.getIdActivite().intValue()){
                            tmp = a;
                            System.out.println("Found: " + tmp);
                            break;
                        }
                    }
                    
                    if(tmp == null){
                        System.exit(100);
                    }
                    
                    //Affichage
                    DefaultTableModel model = (DefaultTableModel) ActivitiesTable.getModel();
                    model.setRowCount(0);
                    model.addRow(tmp.toVector());
                    ActivitiesTable.setModel(model);
                break;
                
            case GetListActResponse.BADDB:
                JOptionPane.showMessageDialog(this, "Erreur Base de Donnée: " + ((GetListActResponse) rep).getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                break;
                
            case GetListActResponse.UNKOWN:
            default:
                JOptionPane.showMessageDialog(this, "Erreur Inconnue", "Erreur", JOptionPane.ERROR_MESSAGE);
                break;
        }
    }
    
    private void refreshVoyageursTable() throws IOException, ClassNotFoundException{
        Socket s = new Socket();
        s.connect(address);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        //Envoie Req
        oos.writeObject(new GetListPartRequest(selectActivities));
        
        ObjectInputStream ios = new ObjectInputStream(s.getInputStream());
        //Recep Rep
        Object rep = null;
        
        rep = ios.readObject();
        
        if(rep instanceof TimeOut){
            System.out.println("To Do Timeout refreshActivities");
            return;
        }
        
        if(!(rep instanceof GetListPartResponse)){
            System.out.println("To Do Other things: " + rep.getClass());
            return;
        }
        s.close();
        
        GetListPartResponse listPartResponse = (GetListPartResponse) rep; 
        
        switch(listPartResponse.getCode()){
        case GetListPartResponse.SUCCESS: 
                    listVoyInscrit = listPartResponse.getList();
                    //Affichage
                    DefaultTableModel model = (DefaultTableModel) VoyageursTable.getModel();
                    model.setRowCount(0);
                    for(Voyageurs v : listVoyInscrit ){
                        model.addRow(v.toVector());
                    }
                    VoyageursTable.setModel(model);
                break;
                
            case GetListPartResponse.BADDB:
                JOptionPane.showMessageDialog(this, "Erreur Base de Donnée: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                break;
                
            case GetListPartResponse.UNKOWN:
            default:
                JOptionPane.showMessageDialog(this, "Erreur Inconnue: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                break;
        }
    }
    
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        Ajouter_Participant = new javax.swing.JButton();
        jScrollPane1 = new javax.swing.JScrollPane();
        ActivitiesTable = new javax.swing.JTable();
        jLabel1 = new javax.swing.JLabel();
        jLabel2 = new javax.swing.JLabel();
        Supprimer_Participant = new javax.swing.JButton();
        jScrollPane2 = new javax.swing.JScrollPane();
        VoyageursTable = new javax.swing.JTable();

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);

        Ajouter_Participant.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Ajouter_Participant.setText("Ajouter Participant");
        Ajouter_Participant.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                Ajouter_ParticipantActionPerformed(evt);
            }
        });

        ActivitiesTable.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {
                "Nom Activitée", "Limite Participant", "Nombre Participant", "Date de début", "Duree Activitée", "Prix (HTVA)"
            }
        ) {
            Class[] types = new Class [] {
                java.lang.String.class, java.lang.String.class, java.lang.String.class, java.lang.String.class, java.lang.String.class, java.lang.Object.class
            };
            boolean[] canEdit = new boolean [] {
                false, false, false, false, false, false
            };

            public Class getColumnClass(int columnIndex) {
                return types [columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        ActivitiesTable.getTableHeader().setReorderingAllowed(false);
        jScrollPane1.setViewportView(ActivitiesTable);

        jLabel1.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        jLabel1.setText("Participants :");

        jLabel2.setFont(new java.awt.Font("Tahoma", 0, 18)); // NOI18N
        jLabel2.setText("Activitée:");

        Supprimer_Participant.setFont(new java.awt.Font("Tahoma", 0, 14)); // NOI18N
        Supprimer_Participant.setText("Supprimer Participant");
        Supprimer_Participant.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                Supprimer_ParticipantActionPerformed(evt);
            }
        });

        VoyageursTable.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {
                "Nom", "Prenom", "Adresse", "Nationalité", "Date Naissance"
            }
        ) {
            Class[] types = new Class [] {
                java.lang.String.class, java.lang.String.class, java.lang.String.class, java.lang.String.class, java.lang.String.class
            };
            boolean[] canEdit = new boolean [] {
                false, false, false, false, false
            };

            public Class getColumnClass(int columnIndex) {
                return types [columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        VoyageursTable.getTableHeader().setReorderingAllowed(false);
        jScrollPane2.setViewportView(VoyageursTable);
        if (VoyageursTable.getColumnModel().getColumnCount() > 0) {
            VoyageursTable.getColumnModel().getColumn(2).setMinWidth(250);
            VoyageursTable.getColumnModel().getColumn(2).setPreferredWidth(250);
        }

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jScrollPane1)
                            .addComponent(jScrollPane2, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 890, Short.MAX_VALUE)
                            .addGroup(layout.createSequentialGroup()
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel2)
                                    .addComponent(jLabel1))
                                .addGap(0, 0, Short.MAX_VALUE)))
                        .addContainerGap())
                    .addGroup(layout.createSequentialGroup()
                        .addGap(84, 84, 84)
                        .addComponent(Ajouter_Participant, javax.swing.GroupLayout.PREFERRED_SIZE, 178, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(Supprimer_Participant, javax.swing.GroupLayout.PREFERRED_SIZE, 178, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(131, 131, 131))))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel2)
                .addGap(5, 5, 5)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 48, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel1)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 376, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(11, 11, 11)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(Ajouter_Participant, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(Supprimer_Participant, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void Ajouter_ParticipantActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_Ajouter_ParticipantActionPerformed
        LinkedList<Voyageurs> listvoy = null;
        LinkedList<Voyageurs> DisplayedVoy = new LinkedList<Voyageurs>();
        DefaultComboBoxModel model = new DefaultComboBoxModel();
        
        try {
            //Request clients:
            listvoy = this.getClients();
            //Reception de tout les clients:
        } catch (IOException | ClassNotFoundException ex) {
            Logger.getLogger(Client_Activities.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        //Retrait des clients déjà inscrits:
        for(Voyageurs v1 : listvoy){
            boolean found = false;
            for(Voyageurs v2 : listVoyInscrit){
                if(v1.getNumeroClient().intValue() == v2.getNumeroClient().intValue()){
                    found = true;
                    break;
                }
            }
            
            if(found == true) continue;
            else DisplayedVoy.add(v1);
        }
        
        //Création d'un vecteur de string similaire:
        for(Voyageurs v : DisplayedVoy){
            model.addElement("n°" + v.getNumeroClient() + ", " + v.getNomVoyageur() + ", " + v.getPrenomVoyageur());
        }
        
        JComboBox comboBox = new JComboBox();
        comboBox.setModel(model);
        
        JOptionPane.showMessageDialog(this, comboBox, "Choissez un nouveau client a ajouter: ", JOptionPane.QUESTION_MESSAGE);
        int retval = comboBox.getSelectedIndex();
        if(retval == -1) return;
        
        //En fonction d'index Envoie Requete Add
        Voyageurs toAdd = DisplayedVoy.get(retval);
        try {
            this.registerActivities(toAdd, false);
        } catch (IOException | ClassNotFoundException ex) {
            Logger.getLogger(Client_Activities.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_Ajouter_ParticipantActionPerformed

    private void registerActivities(Voyageurs toAdd, boolean isPayed) throws IOException, ClassNotFoundException{
        Socket s = new Socket();
        s.connect(address);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        //Envoie Req
        oos.writeObject(new RegisterRequest(selectActivities,toAdd,isPayed));
        
        ObjectInputStream ios = new ObjectInputStream(s.getInputStream());
        //Recep Rep
        Object rep = null;
        
        rep = ios.readObject();
        
        if(rep instanceof TimeOut){
            System.out.println("To Do Timeout refreshActivities");
            return;
        }
        
        if(!(rep instanceof RegisterResponse)){
            System.out.println("To Do Other things: " + rep.getClass());
            return;
        }
        s.close();
        
        RegisterResponse listPartResponse = (RegisterResponse) rep; 
        
        switch(listPartResponse.getCode()){
        case RegisterResponse.SUCCESS:
                JOptionPane.showMessageDialog(this, "Ajout avec succès", "Success", JOptionPane.INFORMATION_MESSAGE);
                this.refreshVoyageursTable();
                return;
                
            case RegisterResponse.BADDB:
                JOptionPane.showMessageDialog(this, "Erreur Base de Donnée: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                return;
                
            case RegisterResponse.UNKOWN:
            default:
                JOptionPane.showMessageDialog(this, "Erreur Inconnue: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                return;
        }
    }
    
    private LinkedList<Voyageurs> getClients() throws IOException, ClassNotFoundException{
        Socket s = new Socket();
        s.connect(address);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        //Envoie Req
        oos.writeObject(new GetListClientRequest());
        
        ObjectInputStream ios = new ObjectInputStream(s.getInputStream());
        //Recep Rep
        Object rep = null;
        
        rep = ios.readObject();
        
        if(rep instanceof TimeOut){
            System.out.println("To Do Timeout refreshActivities");
            return new LinkedList<Voyageurs>();
        }
        
        if(!(rep instanceof GetListClientResponse)){
            System.out.println("To Do Other things: " + rep.getClass());
            return new LinkedList<Voyageurs>();
        }
        s.close();
        
         GetListClientResponse listPartResponse = (GetListClientResponse) rep; 
        
        switch(listPartResponse.getCode()){
        case GetListClientResponse.SUCCESS:
                return listPartResponse.getList();
                
            case GetListClientResponse.BADDB:
                JOptionPane.showMessageDialog(this, "Erreur Base de Donnée: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                return new LinkedList<Voyageurs>();
                
            case GetListClientResponse.UNKOWN:
            default:
                JOptionPane.showMessageDialog(this, "Erreur Inconnue: " + listPartResponse.getMessage(), "Erreur", JOptionPane.ERROR_MESSAGE);
                return new LinkedList<Voyageurs>();
        }
    }
    
    private void Supprimer_ParticipantActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_Supprimer_ParticipantActionPerformed
        int selected = VoyageursTable.getSelectedRow();
        if(selected <0){
            return;
        }
        
        int retval = JOptionPane.showConfirmDialog(this, "Etes vous sur de vouloir supprimer ?");
        if(retval != 0){
            return;
        }
        //On envoie la requète de supression:
        
        //On reçois et traite les réponses:
        
    }//GEN-LAST:event_Supprimer_ParticipantActionPerformed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JTable ActivitiesTable;
    private javax.swing.JButton Ajouter_Participant;
    private javax.swing.JButton Supprimer_Participant;
    private javax.swing.JTable VoyageursTable;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    // End of variables declaration//GEN-END:variables
}