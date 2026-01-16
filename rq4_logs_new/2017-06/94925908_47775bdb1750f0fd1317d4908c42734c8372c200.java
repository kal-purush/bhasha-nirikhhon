import java.awt.* ;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;


/**
 * Created by Danyo on 2017-06-20.
 */
public class MapViewer implements ActionListener {

    JFrame mainWindow ; //window

    JPanel outerPane ;
    JPanel innerPane ;
    JPanel cp ;

    JMenuBar menuBar ;


    JToolBar toolBar ;
    JButton  newBtn ;
    JButton  opnBtn ;
    JButton  saveBtn ;
    JButton  clrBtn ;

    boolean borderedBtns ;
    boolean compactBtns ;

    public MapViewer() {
        mainWindow = new JFrame() ;
        mainWindow.setTitle("EO Mapper") ;

        mainWindow.setSize(new Dimension(800, 600)) ;
        mainWindow.setLocationRelativeTo(null) ;
        mainWindow.setVisible(true) ;
        mainWindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE) ;

        outerPane = (JPanel) mainWindow.getContentPane() ;

        outerPane.setLayout(new BorderLayout()) ;


        newBtn = makeBtn("New", "../img/new.png", "New File") ;

        toolBar = new JToolBar() ;
        toolBar.add(newBtn) ;

        outerPane.add(toolBar, BorderLayout.NORTH) ;



    }

    public JButton makeBtn(String text, String img, String toolTip) {
        JButton button ;

        try {
            button = new JButton(new ImageIcon(getClass().getResource(img))) ;
        }

        catch (Exception e) {
            button = new JButton(text) ;
        }

        button.setToolTipText(toolTip) ;
        button.addActionListener(this) ;

        if (borderedBtns) {
            button.setBorder(new LineBorder(Color.gray, 1, false)) ;
        }
        else if (compactBtns) {
            button.setBorder(new EmptyBorder( 0, 0, 0, 0));
        }

        return button ;
    }

    public void actionPerformed(ActionEvent e) {

    }
}