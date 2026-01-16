package com.javalec.dialog;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import com.javalec.util.DBConnect;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import javax.swing.JLabel;
import javax.swing.JOptionPane;

import java.awt.Font;
import javax.swing.JTextField;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.awt.event.ActionEvent;

public class AdditionStore extends JDialog {

	private final JPanel contentPanel = new JPanel();
	private JLabel lblStoreSeq2;
	private JLabel lblSname;
	private JLabel lblSaddress;
	private JLabel lblStelno;
	private JLabel lblEid;
	private JLabel lblEname;
	private JLabel lblSopendate;
	private JLabel lblScrn;
	private JTextField tfStoreseq2;
	private JTextField tfSname;
	private JTextField tfSaddress;
	private JTextField tfStelno;
	private JTextField tfEid;
	private JTextField tfEname;
	private JTextField tfSopendate;
	private JTextField tfScrn;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		try {
			AdditionStore dialog = new AdditionStore();
			dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
			dialog.setVisible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create the dialog.
	 */
	public AdditionStore() {
		setTitle("매장 등록");
		setBounds(100, 100, 601, 434);
		getContentPane().setLayout(new BorderLayout());
		contentPanel.setBorder(new EmptyBorder(5, 5, 5, 5));
		getContentPane().add(contentPanel, BorderLayout.CENTER);
		contentPanel.setLayout(null);
		contentPanel.add(getLblStoreSeq2());
		contentPanel.add(getLblSname());
		contentPanel.add(getLblSaddress());
		contentPanel.add(getLblStelno());
		contentPanel.add(getLblEid());
		contentPanel.add(getLblEname());
		contentPanel.add(getLblSopendate());
		contentPanel.add(getLblScrn());
		contentPanel.add(getTfStoreseq2());
		contentPanel.add(getTfSname());
		contentPanel.add(getTfSaddress());
		contentPanel.add(getTfStelno());
		contentPanel.add(getTfEid());
		contentPanel.add(getTfEname());
		contentPanel.add(getTfSopendate());
		contentPanel.add(getTfScrn());
		{
			JPanel buttonPane = new JPanel();
			buttonPane.setLayout(new FlowLayout(FlowLayout.RIGHT));
			getContentPane().add(buttonPane, BorderLayout.SOUTH);
			{
				JButton btnInsert = new JButton("등록");
				btnInsert.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						insertAction();

					}
				});
				btnInsert.setActionCommand("OK");
				buttonPane.add(btnInsert);
				getRootPane().setDefaultButton(btnInsert);
			}
		}
	}

	private JLabel getLblStoreSeq2() {
		if (lblStoreSeq2 == null) {
			lblStoreSeq2 = new JLabel("매장코드");
			lblStoreSeq2.setFont(new Font("Gulim", Font.BOLD, 14));
			lblStoreSeq2.setBounds(30, 30, 67, 17);
		}
		return lblStoreSeq2;
	}

	private JLabel getLblSname() {
		if (lblSname == null) {
			lblSname = new JLabel("매장명");
			lblSname.setFont(new Font("Gulim", Font.BOLD, 14));
			lblSname.setBounds(30, 70, 67, 17);
		}
		return lblSname;
	}

	private JLabel getLblSaddress() {
		if (lblSaddress == null) {
			lblSaddress = new JLabel("주소");
			lblSaddress.setFont(new Font("Gulim", Font.BOLD, 14));
			lblSaddress.setBounds(30, 110, 67, 17);
		}
		return lblSaddress;
	}

	private JLabel getLblStelno() {
		if (lblStelno == null) {
			lblStelno = new JLabel("전화번호");
			lblStelno.setFont(new Font("Gulim", Font.BOLD, 14));
			lblStelno.setBounds(30, 150, 67, 17);
		}
		return lblStelno;
	}

	private JLabel getLblEid() {
		if (lblEid == null) {
			lblEid = new JLabel("지점장ID");
			lblEid.setFont(new Font("Gulim", Font.BOLD, 14));
			lblEid.setBounds(30, 190, 67, 17);
		}
		return lblEid;
	}

	private JLabel getLblEname() {
		if (lblEname == null) {
			lblEname = new JLabel("지점장명");
			lblEname.setFont(new Font("Gulim", Font.BOLD, 14));
			lblEname.setBounds(30, 240, 67, 17);
		}
		return lblEname;
	}

	private JLabel getLblSopendate() {
		if (lblSopendate == null) {
			lblSopendate = new JLabel("개업일");
			lblSopendate.setFont(new Font("Gulim", Font.BOLD, 14));
			lblSopendate.setBounds(30, 280, 67, 17);
		}
		return lblSopendate;
	}

	private JLabel getLblScrn() {
		if (lblScrn == null) {
			lblScrn = new JLabel("사업자등록번호");
			lblScrn.setFont(new Font("Gulim", Font.BOLD, 14));
			lblScrn.setBounds(30, 320, 108, 17);
		}
		return lblScrn;
	}

	private JTextField getTfStoreseq2() {
		if (tfStoreseq2 == null) {
			tfStoreseq2 = new JTextField();
			tfStoreseq2.setBounds(150, 30, 100, 21);
			tfStoreseq2.setColumns(10);
		}
		return tfStoreseq2;
	}

	private JTextField getTfSname() {
		if (tfSname == null) {
			tfSname = new JTextField();
			tfSname.setColumns(10);
			tfSname.setBounds(150, 70, 150, 21);
		}
		return tfSname;
	}

	private JTextField getTfSaddress() {
		if (tfSaddress == null) {
			tfSaddress = new JTextField();
			tfSaddress.setColumns(10);
			tfSaddress.setBounds(150, 110, 400, 21);
		}
		return tfSaddress;
	}

	private JTextField getTfStelno() {
		if (tfStelno == null) {
			tfStelno = new JTextField();
			tfStelno.setColumns(10);
			tfStelno.setBounds(150, 150, 140, 21);
		}
		return tfStelno;
	}

	private JTextField getTfEid() {
		if (tfEid == null) {
			tfEid = new JTextField();
			tfEid.setColumns(10);
			tfEid.setBounds(150, 190, 80, 21);
		}
		return tfEid;
	}

	private JTextField getTfEname() {
		if (tfEname == null) {
			tfEname = new JTextField();
			tfEname.setColumns(10);
			tfEname.setBounds(150, 240, 110, 21);
		}
		return tfEname;
	}

	private JTextField getTfSopendate() {
		if (tfSopendate == null) {
			tfSopendate = new JTextField();
			tfSopendate.setColumns(10);
			tfSopendate.setBounds(150, 280, 106, 21);
		}
		return tfSopendate;
	}

	private JTextField getTfScrn() {
		if (tfScrn == null) {
			tfScrn = new JTextField();
			tfScrn.setColumns(10);
			tfScrn.setBounds(150, 320, 200, 21);
		}
		return tfScrn;
	}

	private int insertAction() { // CUD은 다 같음, SQL 코드만 다름
		PreparedStatement ps = null; // sql 코드 에러 등
		int check = 0;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection conn_mysql = DriverManager.getConnection(DBConnect.url_mysql, DBConnect.id_mysql,
					DBConnect.pw_mysql);
			String query = "insert into store (storeseq2, sname, saddress, stelno, eid, ename, scrn, sopendate) ";
			String query1 = "values (?, ?, ?, ?, ?, ?, ?, ?)";

			ps = conn_mysql.prepareStatement(query + query1);
			ps.setString(1, tfStoreseq2.getText().trim());
			ps.setString(2, tfSname.getText().trim());
			ps.setString(3, tfSaddress.getText().trim());
			ps.setString(4, tfStelno.getText().trim());
			ps.setString(5, tfEid.getText().trim());
			ps.setString(6, tfEname.getText().trim());
			ps.setString(7, tfScrn.getText().trim());
			ps.setString(8, tfSopendate.getText().trim());
			ps.executeUpdate(); // 0 정상일 시

			conn_mysql.close();

			JOptionPane.showMessageDialog(null, tfSname.getText() + "님의 정보가 입력 되었습니다.!");

		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println(check);
		return check; // 값 정상 입력 체크

	}

} // End