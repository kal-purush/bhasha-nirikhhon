import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

public class AWindow extends JFrame {
    private JPanel panel1;
    private GPanel panel2;
    private JPanel panel3;
    private JPanel panel4;
    private JScrollPane scrollPane;
    private JTextArea textArea;
    private JSplitPane splitPane;
    private JMenuBar menuBar;
    private JMenu menu;
    private JMenuItem menuItem;

    public AWindow(){
        super();

        GridBagConstraints gbc = new GridBagConstraints();
        getContentPane().setLayout(new GridBagLayout());

        textArea = new JTextArea();
        //textArea.
        scrollPane = new JScrollPane(textArea, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setMinimumSize(new Dimension(-1, 100));
        //scrollPane.setPreferredSize(new Dimension(300, 100));
        //textArea.setPreferredSize(new Dimension(300, 100));
        textArea.setMinimumSize(new Dimension(300, 100));
        //textArea.setRows(3);
        textArea.setText("ABOBAaaaaaaaaaaaa");
        //textArea.setEditable(false);
        panel1 = new JPanel();
        panel1.setLayout(new GridBagLayout());
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets.set(3,3,3,3);
        gbc.weightx = 1f;
        gbc.weighty = 1f;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        panel1.add(scrollPane, gbc);
        panel1.setBackground(Color.RED);

        panel2 = new GPanel(new Solver());
        panel2.setMinimumSize(new Dimension(-1, -1));
        panel2.setBackground(Color.YELLOW);

        panel3 = new JPanel();
        panel3.setBackground(Color.GREEN);

        panel4 = new JPanel();
        panel4.setBackground(Color.MAGENTA);


        menuBar = new JMenuBar();
        menu = new JMenu("File");
        menuItem = new JMenuItem("Close");
        menu.add(menuItem);
        menuBar.add(menu);
        //gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weighty = 0f;
        //gbc.weightx = 1f;
        gbc.insets.set(3,3,3,3);
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.anchor = GridBagConstraints.NORTH;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        getContentPane().add(menuBar, gbc);

        gbc.anchor = GridBagConstraints.CENTER;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridwidth = 1;

        splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        splitPane.setTopComponent(panel1);
        splitPane.setBottomComponent(panel2);
        splitPane.setDividerLocation(100);

        gbc.fill = GridBagConstraints.BOTH;
        //gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.weighty = 1f;
        gbc.weightx = 1f;
        getContentPane().add(splitPane, gbc);

        panel3.setPreferredSize(new Dimension(200, -1));
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.weighty = 1f;
        gbc.weightx = 0f;
        gbc.gridheight = 2;
        getContentPane().add(panel3, gbc);

        gbc.gridheight = 1;

        panel4.add(new JButton("Button"));
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.weighty = 0f;
        gbc.weightx = 1f;
        getContentPane().add(panel4, gbc);

        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(500, 500));
        setVisible(true);
    }

    public static void main(String[] args){
        new AWindow();
    }
}