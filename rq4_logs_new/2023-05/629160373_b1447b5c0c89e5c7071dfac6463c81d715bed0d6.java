import javax.swing.*;
import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Map extends JFrame{
    private ImageIcon map,buttonIcon1,buttonIcon2;
    private JLabel imageLabel,dayLabel,dateLabel;
    private JPanel dateLayer;
    private SimpleDateFormat dayFormat,dateFormat;

    private JButton one,two,three,four,five,six,seven,eight,nine,ten,eleven,twelve,thirteen,fourteen,fifteen,sixteen;
    public Map(){

        this.setTitle("Buy Ticket");
        this.setSize(980,580);
        this.setLayout(null);
        this.setLocationRelativeTo(null);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setResizable(false);

        ImageIcon logo = new ImageIcon(getClass().getResource("/images/logo1.png"));
        this.setIconImage(logo.getImage());

        map = new ImageIcon(getClass().getResource("/images/map.png"));
        imageLabel = new JLabel();
        imageLabel.setIcon(map);
        imageLabel.setBounds(0,0,980,550);
        this.add(imageLabel);

        buttonIcon1 = new ImageIcon(getClass().getResource("/images/red_button2.jpg"));
        buttonIcon2 = new ImageIcon(getClass().getResource("/images/red_button3.jpg"));
        one = new JButton();
        one.setIcon(buttonIcon1);
        one.setBounds(76,457,20,20);
        this.add(one);

        two = new JButton();
        two.setIcon(buttonIcon1);
        two.setBounds(138,478,20,20);
        this.add(two);

        three = new JButton();
        three.setIcon(buttonIcon1);
        three.setBounds(201,485,20,20);
        this.add(three);

        four = new JButton();
        four.setIcon(buttonIcon1);
        four.setBounds(298,490,20,20);
        this.add(four);

        five = new JButton();
        five.setIcon(buttonIcon1);
        five.setBounds(364,483,20,20);
        this.add(five);

        six = new JButton();
        six.setIcon(buttonIcon1);
        six.setBounds(420,470,20,20);
        this.add(six);

        seven = new JButton();
        seven.setIcon(buttonIcon1);
        seven.setBounds(482,447,20,20);
        this.add(seven);

        eight= new JButton();
        eight.setIcon(buttonIcon1);
        eight.setBounds(542,422,20,20);
        this.add(eight);

        nine = new JButton();
        nine.setIcon(buttonIcon1);
        nine.setBounds(602,405,20,20);
        this.add(nine);

        ten = new JButton();
        ten.setIcon(buttonIcon2);
        ten.setBounds(650,394,20,20);
        this.add(ten);

        eleven = new JButton();
        eleven.setIcon(buttonIcon2);
        eleven.setBounds(719,352,20,20);
        this.add(eleven);

        twelve = new JButton();
        twelve.setIcon(buttonIcon2);
        twelve.setBounds(778,325,20,20);
        this.add(twelve);

        thirteen = new JButton();
        thirteen.setIcon(buttonIcon2);
        thirteen.setBounds(838,320,20,20);
        this.add(thirteen);

        fourteen = new JButton();
        fourteen.setIcon(buttonIcon2);
        fourteen.setBounds(875,320,20,20);
        this.add(fourteen);

        fifteen = new JButton();
        fifteen.setIcon(buttonIcon2);
        fifteen.setBounds(895,250,20,20);
        this.add(fifteen);

        sixteen = new JButton();
        sixteen.setIcon(buttonIcon2);
        sixteen.setBounds(908,180,20,20);
        this.add(sixteen);

        //aikhan theke ektu dekhis

        dateLayer = new JPanel();
        dateLayer.setBounds(0,552,980,28);
        dateLayer.setBackground(new Color(126, 230, 228));
        this.add(dateLayer);

        dayFormat = new SimpleDateFormat("EEEE");
        dayLabel = new JLabel();
        dayLabel.setFont(new Font("Ink Free",Font.BOLD,22));
        String day = dayFormat.format(Calendar.getInstance().getTime());
        dayLabel.setText(day);
//        dayLabel.setHorizontalAlignment(JLabel.LEFT);//why those
        dayLabel.setVerticalTextPosition(JLabel.TOP);//aren't working
        dateLayer.add(dayLabel);

        dateFormat = new SimpleDateFormat("dd, MM, yyyy");
        dateLabel =new JLabel();
        dateLabel.setFont(new Font("Ink Free",Font.BOLD,22));
        String date= dateFormat.format(Calendar.getInstance().getTime());
        dateLabel.setText(date);
        dateLayer.add(dateLabel);


        this.setVisible(true);
    }

    public static void main(String[] args) {
        Map m= new Map();
    }
}