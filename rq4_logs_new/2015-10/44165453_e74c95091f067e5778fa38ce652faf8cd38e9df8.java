/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Client;

import CentralPoint.ConstantTags;
import CentralPoint.DeXMLlize;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JViewport;

/**
 *
 * @author sonng_000
 */
public class ClientFromClient implements Runnable {

    private DataInputStream in;
    private String peerName;
    private ArrayList<Entry> lstTabChat;
    private ClientFrame peerChat;
    private JTabbedPane tabPanel;
    
    public ClientFromClient(ClientFrame peerChat, DataInputStream in, String peerName, ArrayList<Entry> lstTabChat, JTabbedPane tabPanel) {
        this.in = in;
        this.peerName = peerName;
        this.lstTabChat = lstTabChat;
        this.peerChat = peerChat;
        this.tabPanel = tabPanel;
    }
    
    @Override
    public void run() {
        String msg = "";
            DeXMLlize xml;
            while (true) {
                try {
                    msg = in.readUTF();
                } catch (IOException ex) {
                    Logger.getLogger(ClientFromClient.class.getName()).log(Level.SEVERE, null, ex);
                }
                xml = new DeXMLlize(msg);
                switch (xml.firstTag()) {
                    case ConstantTags.CHAT_MSG_TAG:
                    {
                        try {
                            retrieveTxt(findTab(peerName)).append(peerName + ": " + xml.getMessage().getMessage() + "\n");
                        } catch (Exception ex) {
                            Logger.getLogger(ClientFromClient.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                        break;
                    case ConstantTags.CHAT_CLOSE_TAG:
                        for (Entry tmp : lstTabChat) {
                            if (tmp.username == peerName) {
                                tmp.availableToChat = false;
                                break;
                            }
                        }
                        if (lstTabChat.get(tabPanel.getSelectedIndex()).username == peerName)
                        {
                            peerChat.btnSend.setEnabled(false);
                            peerChat.btnTransfer.setEnabled(false);
                        }
                        retrieveTxt(findTab(peerName)).append(peerName + " has just logged out");
                        break;
                    case ConstantTags.FILE_REQ_TAG:
                        
                        break;
                }
            }
    }
    
    private JPanel findTab(String peername) {
        for (Entry tmp : lstTabChat) {
            if (tmp.username.equals(peername)) {
                return tmp.jp;
            }
        }
        return null;
    }
    
    private JTextArea retrieveTxt(JPanel jp) {
        JScrollPane j = (JScrollPane) jp.getComponent(0);
        JViewport vp = (JViewport) j.getComponent(0);
        JTextArea txtArea = (JTextArea) vp.getComponent(0);
        return txtArea;
    }
    
}