import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class TicketingPortal extends JFrame implements ActionListener {

    User user;
    int distancefactor;
    JTextArea personalInfo;
    String initLocation;
    String destination;
    JPanel panel1,panel2,panel3;
    JLabel personal,ticket,price , iLocation,destin , unit,total;
    JTextField ticketNumber;
    JButton plus,minus,next,confirm;
    int ticketCount = 0 ,unitprice = 20 , totalCost;


    public TicketingPortal(String initLocation,String destination,int distancefactor,User user){
        this.initLocation = initLocation;
        this.destination = destination;
        this.distancefactor=distancefactor;
        this.user=user;


        personalInfo = new JTextArea();
        personalInfo.setBounds(30,70,160,200);
        personalInfo.setEditable(false);
        personalInfo.setFont(new Font("Arial",Font.BOLD,20));
        personalInfo.setText(user.getName()+"\n"+user.getEmail()+"\n"+user.getAddress()+
                "\n"+user.getNid()+"\n"+user.getNationality());

        iLocation = new JLabel();
        iLocation.setText("From: \n"+initLocation);
        iLocation.setFont(new Font("Arial",Font.BOLD,20));
        iLocation.setBounds(30,70,100,50);

        unit = new JLabel();
        unit.setText("Unit Price: "+unitprice +" tk");
        unit.setFont(new Font("Arial",Font.BOLD,18));
        unit.setBounds(30,120,160,50);
        unit.setOpaque(true);

        total = new JLabel();
        total.setFont(new Font("Arial",Font.BOLD,18));
        total.setBounds(30,190,160,50);
        total.setOpaque(true);

        ticketNumber = new JTextField();
        ticketNumber.setBounds(10,190,100,30);
        //ticketNumber.setEnabled(false);
        ticketNumber.setText(String.valueOf(ticketCount));

        plus = new JButton("+");
        plus.addActionListener(this);
        plus.setFont(new Font("Arial",Font.BOLD,24));
        plus.setBounds(120,190,50,30);
        plus.setFocusable(false);

        minus = new JButton("-");
        minus.addActionListener(this);
        minus.setFont(new Font("Arial",Font.BOLD,24));
        minus.setBounds(180,190,50,30);
        minus.setFocusable(false);

        destin = new JLabel();
        destin.setText("To: \n"+destination);
        destin.setFont(new Font("Arial",Font.BOLD,20));
        destin.setBounds(30,120,100,50);

        next = new JButton("Next");
        next.addActionListener(this);
        next.setBounds(60,240,90,30);

        panel1 = new JPanel();
        panel1.setBounds(30,70,230,350);
        panel1.setLayout(null);
        panel1.setBackground(Color.CYAN);

        panel2 = new JPanel();
        panel2.setBounds(285,70,230,350);
        panel2.setLayout(null);
        panel2.setBackground(Color.CYAN);

        panel3 = new JPanel();
        panel3.setBounds(530,70,230,350);
        panel3.setLayout(null);
        panel3.setBackground(Color.BLACK);
        panel3.setVisible(false);

        personal = new JLabel();
        personal.setText("Personal Details");
        personal.setFont(new Font("Arial",Font.BOLD,20));
        personal.setBounds(30,10,160,30);
        personal.setOpaque(true);

        ticket = new JLabel();
        ticket.setText("Ticket Amount");
        ticket.setFont(new Font("Arial",Font.BOLD,20));
        ticket.setBounds(30,10,160,30);
        ticket.setOpaque(true);

        price = new JLabel();
        price.setText("Purchase");
        price.setFont(new Font("Arial",Font.BOLD,20));
        price.setBounds(50,10,130,30);
        price.setOpaque(true);


        panel1.add(personal);
        panel1.add(personalInfo);

        panel2.add(ticket);
        panel2.add(iLocation);
        panel2.add(destin);
        panel2.add(ticketNumber);
        panel2.add(plus);
        panel2.add(minus);
        panel2.add(next);

        panel3.add(price);
        panel3.add(unit);
        panel3.add(total);

        add(panel1);
        add(panel2);
        add(panel3);

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setTitle("Ticketing Portal");
        setLayout(null);
        setSize(800,500);
        setLocationRelativeTo(null);
        setVisible(true);
    }

    public static void main(String[] args) {
        new TicketingPortal("hi","hello",6,new User("shams","dkfls",
                "sjdllkd","male","654654","lsdfl","lksd","lskd"));

    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource()==plus){
            if (ticketCount>=0&&ticketCount<=4){
                ticketCount++;
                ticketNumber.setText(String.valueOf(ticketCount));
            }

        }
        if (e.getSource()==minus){
            if (ticketCount>=1&&ticketCount<=5){
                ticketCount--;
                ticketNumber.setText(String.valueOf(ticketCount));
            }
        }
        if (e.getSource()==next){
            totalCost = distancefactor*ticketCount*unitprice;
            total.setText("total Price: "+totalCost +" tk");
            plus.setEnabled(false);
            minus.setEnabled(false);
            ticketNumber.setEnabled(false);
            panel3.setVisible(true);
        }
    }
}