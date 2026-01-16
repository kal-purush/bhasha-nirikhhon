package chatServer;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.DefaultTableModel;

public class MemSearchView extends JFrame implements ActionListener,MouseListener, FocusListener{
	JPanel			  jp_north 	   = new JPanel();
	JButton 		  jbtn_sel 	   = new JButton("조회");
	JButton 		  jbtn_del 	   = new JButton("삭제");
	JLabel			  jb_id	  	   = new JLabel("아이디");
	String 			  zdos[]   	   = {"아이디","이름","전화번호"};
	JComboBox		  jcb	   	   = new JComboBox(zdos);
	JTextField		  jtf_search   = new JTextField("검색 조건을 선택하고 입력하세요");
	JLabel			  jb_phone	   = new JLabel("");
	JLabel			  jb_name	   = new JLabel("");
	String cols[] = {"아이디","이름","전화번호"}; // header부분
	String data[][] = new String[0][3]; //body부분
	DefaultTableModel dtm = new DefaultTableModel(data,cols);
	JTable			  jtb = new JTable(dtm);
	JScrollPane		  jsp  = new JScrollPane(jtb);
	Font			  font = new Font("나눔고딕",Font.BOLD,15);
	
	public MemSearchView() {
		
	}
	
	
	public void initDisplay() {
		jcb.addActionListener(this);
		jtf_search.addActionListener(this);
		jtf_search.addFocusListener(this);
		jbtn_sel.addActionListener(this);
		jp_north.setLayout(new BorderLayout());
		jp_north.add("West",jcb);
		jp_north.add("Center",jtf_search);
		jp_north.add("East",jbtn_sel);
		this.add("North",jp_north);
		this.add("Center",jsp);
		this.setTitle("회원 조회");
		this.setSize(600,500);
		this.setVisible(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setResizable(false);
		}
	
	public static void main(String[]args) {
		MemSearchView ms = new MemSearchView();
		ms.initDisplay();
		
	}


	@Override
	public void actionPerformed(ActionEvent e) {
		Object obj = e.getSource();
		if(jbtn_sel == obj) {
			if(jtf_search.getText() != "") {
				System.out.println("조회 클릭");
				
			}	else {
				JOptionPane.showMessageDialog(this, "검색할 조건을 입력하세요");
			}
				
		}
		
	}


	@Override
	public void mouseClicked(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void mousePressed(MouseEvent e) {
		
	}


	@Override
	public void mouseReleased(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void mouseEntered(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void mouseExited(MouseEvent e) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void focusGained(FocusEvent e) {
		if(e.getSource() == jtf_search) {
			jtf_search.setText("");
			
			
		}
		
	}


	@Override
	public void focusLost(FocusEvent e) {
		// TODO Auto-generated method stub
		
	}
	
}