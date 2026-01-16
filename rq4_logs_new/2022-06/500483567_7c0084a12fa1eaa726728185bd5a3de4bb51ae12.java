package chatClient;

import java.awt.Font;
import java.awt.Graphics;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.Serializable;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
// LoginView
public class Login extends JFrame implements ActionListener {
	//선언부
	String nickName = "";
	String imgPath = "C:\\Users\\MJ\\Desktop\\이미지\\";
	JLabel jlb_id = new JLabel("아이디");
	JLabel jlb_pw = new JLabel("패스워드");
	Font jl_font = new Font("맑은고딕체", Font.BOLD, 14);
	JTextField jtf_id = new JTextField("");
	JPasswordField jpf_pw = new JPasswordField("");
	JButton jbtn_login = new JButton(new ImageIcon(imgPath + "로그인2.png"));
	JButton jbtn_join = new JButton(new ImageIcon(imgPath + "회원가입2.png"));
	// JPanel에 쓰일 이미지아이콘
	ImageIcon ig = new ImageIcon(imgPath + "둥이.png");

	String user_Id; // 톡클라이언트에게 넘기기 위해 ID 전역변수로 설정
	String user_Name; // 톡클라이언트에게 넘기기 위해 닉네임 전역변수로 설정
	LoginDao loginDao;

	public Login() {
		initDisplay();
	}

	/* 배경이미지 */
	class mypanal extends JPanel {
		public void paintComponent(Graphics g) {
			g.drawImage(ig.getImage(), 0, 0, null);
			setOpaque(false);
			super.paintComponents(g);
		}
	}

	// 화면처리
	public void initDisplay() {
		setContentPane(new mypanal());
		/* 버튼과 텍스트필드 구성 */
		/* 회원가입 버튼 */
		jbtn_join.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				if (e.getSource() == jbtn_join) {
					new Sign();
					dispose();
				}
			}
		});
		jbtn_login.addActionListener(this);
		jtf_id.addActionListener(this);
		jpf_pw.addActionListener(this);

		this.setLayout(null);
		this.setTitle("꽉자바 ver.1");
		this.setSize(427, 443);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setVisible(true);
		this.setLocation(600, 150);
		this.setResizable(false);
		this.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		// id 라인
		jlb_id.setBounds(45, 200, 80, 40);
		jlb_id.setFont(jl_font);
		this.add(jlb_id);

		// 아이디 입력창
		jtf_id.setBounds(110, 200, 185, 40);
		this.add(jtf_id);

		// pw 라인
		jlb_pw.setBounds(45, 240, 80, 40);
		jlb_pw.setFont(jl_font);
		this.add(jlb_pw);

		// pw 입력창
		jpf_pw.setBounds(110, 240, 185, 40);
		this.add(jpf_pw);

		// 로그인 버튼 라인
		jbtn_login.setBounds(175, 285, 120, 40);
		this.add(jbtn_login);

		// 회원가입 버튼 라인
		jbtn_join.setBounds(45, 285, 120, 40);
		this.add(jbtn_join);
	}

	public static void main(String[] args) {
		new Login();
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		// JTextField 엔터 이벤트 처리
		if (jtf_id == e.getSource() || jpf_pw == e.getSource() || jbtn_login == e.getSource()) {
			if (!(jtf_id.getText().equals("")) && !(jpf_pw.getText().equals(""))) {
				System.out.println("로그인 호출 성공");
				// 사용자가 화면에 입력하는 아이디를 담기
				user_Id = jtf_id.getText();
				// 사용자가 화면에 입력하는 비번을 담기
				String user_pw = jpf_pw.getText();
				// 오라클 서버에서 반환 값 담기
				int result = 0; // 이름(1) or 0(비번이 틀림) or -1(아이디가 존재하지 않음)
				loginDao = new LoginDao();
				// 사용자가 입력한 아이디와 비번을 Dao클래스의 login메소드에 파라미터로 넘김
				result = loginDao.login(user_Id, user_pw);
				// 위에서 오라클 서버에 요청한 결과를 출력하기
				user_Name = loginDao.name(user_Id);
				System.out.println("result : " + result);

				if (result == 1) {
					this.dispose();
					TalkClient tc = new TalkClient(this);
					tc.initDisplay(tc.is);
					tc.init();
				} else if (result == 0) {
					JOptionPane.showMessageDialog(this, "비밀번호가 틀렸습니다", "ERROR", JOptionPane.ERROR_MESSAGE);
					return;
				} else if (result == -1) {
					JOptionPane.showMessageDialog(this, "존재하지 않는 아이디입니다.", "ERROR", JOptionPane.ERROR_MESSAGE);
					return;
				}

			} else if (jtf_id.getText().equals("")) {
				JOptionPane.showMessageDialog(this, "아이디를 입력해 주세요", "ERROR", JOptionPane.ERROR_MESSAGE);
				return;

			} else if (jpf_pw.getText().equals("")) {
				JOptionPane.showMessageDialog(this, "비밀번호를 입력해 주세요", "ERROR", JOptionPane.ERROR_MESSAGE);
				return;

			}
		}

	}
}