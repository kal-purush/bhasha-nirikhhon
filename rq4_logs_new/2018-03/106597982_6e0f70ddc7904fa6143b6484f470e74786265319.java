package View;

import javax.swing.*;

public class ShowSecrecyAndEfficiency {
    private double secrecy;
    private double efficiency;

    private double initSecrecy;
    private double initEfficiency;
    private double resultSecrecy;
    private double resultEfficiency;

    public ShowSecrecyAndEfficiency(double secrecy, double efficiency){
        this.secrecy = secrecy;
        this.efficiency = efficiency;
    }

    public ShowSecrecyAndEfficiency(double initSecrecy, double initEfficiency, double resultSecrecy, double resultEfficiency){
        this.initSecrecy = initSecrecy;
        this.initEfficiency = initEfficiency;
        this.resultSecrecy = resultSecrecy;
        this.resultEfficiency = resultEfficiency;
    }

    public void showDialogWindow(){
        JFrame frame = new JFrame("Secrecy and Efficiency Measurement");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        JOptionPane.showMessageDialog(frame,
                "Secrecy: In the condition you have set, the network would be destroyed in "+(int)secrecy+" attacks\n" +
                        "Efficiency: It requires "+efficiency+" hours to deliver one message to all other members in this network.",
                "Secrecy and Efficiency Measurement Result",
                JOptionPane.PLAIN_MESSAGE);
    }

    public void showInitAndResDialogWindow(){
        JFrame frame = new JFrame("Secrecy and Efficiency Measurement");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        JOptionPane.showMessageDialog(frame,
                "Initial Network:\n"+"Secrecy: In the condition you have set, the network would be destroyed in "+(int)initSecrecy+" attacks\n" +
                        "Efficiency: It requires "+initEfficiency+" hours to deliver one message to all other members in this network.\n"+
                        "\nCovert Network:\n"+"Secrecy: In the condition you have set, the network would be destroyed in "+(int)resultSecrecy+" attacks\n" +
                        "Efficiency: It requires "+resultEfficiency+" hours to deliver one message to all other members in this network.",
                "Comparison of Initial Network and Covert Network",
                JOptionPane.PLAIN_MESSAGE);
    }
}