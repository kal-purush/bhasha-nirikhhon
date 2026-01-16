import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import java.awt.*;

public class BoxPendapatan extends JPanel {

    static JTextField pendapatantotal = new JTextField();
    static JLabel labelpendapatan = new JLabel("Total Pendapatan");
    static JTable tabelpendapatan = new JTable();

    public BoxPendapatan(){
        Database db = new Database();
        db.tabel_parkir();

        add(labelpendapatan);
        add(pendapatantotal);
        JScrollPane scrollPane = new JScrollPane(tabelpendapatan);
        tabelpendapatan.setFillsViewportHeight(true);
        tabelpendapatan.setEnabled(true);
        pendapatantotal.setEditable(false);
        pendapatantotal.setBackground(Color.white);


        labelpendapatan.setFont(new Font(null, Font.BOLD, 14));

        setLayout(new GridLayout(1,2));

        Border etchedBorder = BorderFactory.createEtchedBorder(EtchedBorder.LOWERED);

        TitledBorder titledBorder = BorderFactory.createTitledBorder(etchedBorder,"");
        titledBorder.setTitleFont(titledBorder.getTitleFont().deriveFont(Font.BOLD));

        setBorder(BorderFactory.createCompoundBorder(new EmptyBorder(10,100,100,100),titledBorder));
    }
}