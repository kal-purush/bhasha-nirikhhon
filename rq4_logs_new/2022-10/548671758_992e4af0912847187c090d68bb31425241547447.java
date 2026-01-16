package com.javalec.base;

import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Font;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableModel;

import com.javalec.panel.AdminDashboardPanel;
import com.javalec.panel.AdminMenuManagePanel;
import com.javalec.panel.AdminEmployeeManagePanel;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JButton;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;


// 관리자 로그인 시 첫 화면 (프레임)
public class AdminMainFrame {
	

	private JFrame frame;
	private JTextField tfCafeName;
	private JTextField tfShow;
	private JLabel lblSearchBar;
	private JTextField tfSearch;
	private JTextField tfTime;
	

	// panel declaration
	AdminDashboardPanel adminDashboardPanel = new AdminDashboardPanel();
	AdminEmployeeManagePanel adminStoreManagePanel = new AdminEmployeeManagePanel();
	AdminMenuManagePanel adminMenuManagePanel = new AdminMenuManagePanel();
	AdminEmployeeManagePanel adminEmployeeManagePanel = new AdminEmployeeManagePanel();

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					AdminMainFrame window = new AdminMainFrame();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public AdminMainFrame() {
		initialize();
		
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 1280, 720);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		frame.getContentPane().add(getTfCafeName());
		frame.getContentPane().add(getTfShow());
		frame.getContentPane().add(getTfTime());
		frame.getContentPane().add(getLblSearchBar());
		frame.getContentPane().add(getTfSearch());
		frame.getContentPane().add(getPanelAdminMain());
		frame.getContentPane().add(getTfEmployeeManage());
		frame.getContentPane().add(getTfSalesManage());
		frame.getContentPane().add(getTfAdManage());
		frame.getContentPane().add(getTfMemberManage());
		frame.getContentPane().add(getTfInventoryManage());
		frame.getContentPane().add(getTfMenuManage());
		frame.getContentPane().add(getTfNoticeManage());
	
		frame.getContentPane().add(adminDashboardPanel);
		frame.getContentPane().add(getLblBackground());
		frame.getContentPane().add(getTfDashboard());
		tfDashboard.setBackground(new Color(226, 161, 101));
		tfDashboard.setForeground(new Color(255, 255, 255));
		panelAdminMain.setVisible(false);
		adminDashboardPanel.setVisible(true);
		adminDashboardPanel.setBounds(167, 55, 1059, 617);
		adminDashboardPanel.setLayout(null);
		frame.getContentPane().add(getLblAlarmIcons());
		frame.getContentPane().add(getLblProfilePicture());
		frame.getContentPane().add(getTfAdminName());
		frame.getContentPane().add(getTfLoginStore());

	}

	private JTextField getTfCafeName() {
		if (tfCafeName == null) {
			tfCafeName = new JTextField();
			tfCafeName.setEditable(false);
			tfCafeName.setBorder(null);
			tfCafeName.setHorizontalAlignment(SwingConstants.CENTER);
			tfCafeName.setFont(new Font("굴림", Font.BOLD, 32));
			tfCafeName.setText("CafeN");
			tfCafeName.setBounds(0, 0, 130, 46);
			tfCafeName.setColumns(10);
		}
		return tfCafeName;
	}

	private JTextField getTfShow() {
		if (tfShow == null) {
			tfShow = new JTextField();
			tfShow.setBorder(null);

			tfShow.setEditable(false);
			tfShow.setFont(new Font("굴림", Font.PLAIN, 22));
			tfShow.setHorizontalAlignment(SwingConstants.CENTER);
			tfShow.setText("대시보드");
			tfShow.setBounds(138, 8, 95, 36);
			tfShow.setColumns(10);
		}
		return tfShow;
	}

	private JLabel getLblSearchBar() {
		if (lblSearchBar == null) {
			lblSearchBar = new JLabel("New label");
			lblSearchBar
					.setIcon(new ImageIcon(AdminMainFrame.class.getResource("/com/javalec/image/SearchBarIcon.png")));
			lblSearchBar.setBounds(238, 11, 353, 30);
		}
		return lblSearchBar;
	}

	private JTextField getTfSearch() {
		if (tfSearch == null) {
			tfSearch = new JTextField();
			tfSearch.setBounds(241, 11, 330, 30);
			tfSearch.setColumns(10);
		}
		return tfSearch;
	}

	private JTextField getTfTime() {
		if (tfTime == null) {
			tfTime = new JTextField();
			tfTime.setFont(new Font("굴림", Font.PLAIN, 22));
			tfTime.setEditable(false);
			tfTime.setBounds(680, 11, 220, 30);
			tfTime.setColumns(10);
			tfTime.setText(dtf.format(now));
			tfTime.setBorder(null);
		}
		return tfTime;
	}

	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
	LocalDateTime now = LocalDateTime.now();
	private JPanel panelAdminMain;

	private JPanel getPanelAdminMain() {
		if (panelAdminMain == null) {
			panelAdminMain = new JPanel();
			panelAdminMain.setBounds(167, 74, 1059, 617);
		}
		return panelAdminMain;
	}
	private JTextField tfDashboard;
	private JTextField tfSalesManage;
	private JTextField tfEmployeeManage;
	private JTextField tfInventoryManage;
	private JTextField tfMemberManage;
	private JTextField tfNoticeManage;
	private JTextField tfMenuManage;
	private JTextField tfAdManage;
	private JPanel pnAdminMain;
	private JLabel lblBackground;
	private JLabel lblAlarmIcons;
	private JLabel lblProfilePicture;
	private JTextField tfAdminName;
	private JTextField tfLoginStore;
	
	private JTextField getTfLoginStore() {
		if (tfLoginStore == null) {
			tfLoginStore = new JTextField();
			tfLoginStore.setForeground(new Color(226, 161, 101));
			tfLoginStore.setEditable(false);
			tfLoginStore.setBackground(new Color(240, 240, 240));
			tfLoginStore.setFont(new Font("굴림", Font.PLAIN, 22));
			tfLoginStore.setHorizontalAlignment(SwingConstants.CENTER);
			tfLoginStore.setText("신논현점");
			tfLoginStore.setBounds(0, 70, 130, 36);
			tfLoginStore.setColumns(10);
			tfLoginStore.setBorder(null);
		}
		return tfLoginStore;
	}

	private JTextField getTfDashboard() {
		if (tfDashboard == null) {
			tfDashboard = new JTextField();
			tfDashboard.setBorder(null);
			tfDashboard.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseClicked(MouseEvent e) {
					panelAdminMain.setVisible(false);
					adminStoreManagePanel.setVisible(false);
					adminMenuManagePanel.setVisible(false);
					adminEmployeeManagePanel.setVisible(false);

					adminDashboardPanel.setVisible(true);
					adminDashboardPanel.setBounds(167, 55, 1059, 617);
					adminDashboardPanel.setLayout(null);
					frame.getContentPane().add(adminDashboardPanel);
					tfSearch.setVisible(true);
					lblSearchBar.setVisible(true);
					tfDashboard.setBackground(new Color(226, 161, 101));
					tfDashboard.setForeground(new Color(255, 255, 255));
					tfEmployeeManage.setBackground(new Color(240, 240, 240));
					tfEmployeeManage.setForeground(new Color(0, 0, 0));
					tfMenuManage.setBackground(new Color(240, 240, 240));
					tfMenuManage.setForeground(new Color(0, 0, 0));
					tfShow.setText("대시보드");

				}
			});

			tfDashboard.setEditable(false);
			tfDashboard.setText("대시보드");
			tfDashboard.setFont(new Font("굴림", Font.PLAIN, 22));
			tfDashboard.setHorizontalAlignment(SwingConstants.CENTER);
			tfDashboard.setBounds(0, 120, 130, 36);
			tfDashboard.setColumns(10);
		}
		return tfDashboard;
	}

	private JTextField getTfEmployeeManage() {
		if (tfEmployeeManage == null) {
			tfEmployeeManage = new JTextField();
			tfEmployeeManage.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseClicked(MouseEvent e) {
					panelAdminMain.setVisible(false);
					adminDashboardPanel.setVisible(false);
					adminStoreManagePanel.setVisible(false);
					adminMenuManagePanel.setVisible(false);

					adminEmployeeManagePanel.setVisible(true);
					adminEmployeeManagePanel.setBounds(167, 55, 1059, 617);
					adminEmployeeManagePanel.setLayout(null);
					frame.getContentPane().add(adminEmployeeManagePanel);
					tfSearch.setVisible(false);
					lblSearchBar.setVisible(false);
					
					
					tfEmployeeManage.setBackground(new Color(226, 161, 101));
					tfEmployeeManage.setForeground(new Color(255, 255, 255));
					
					tfMenuManage.setBackground(new Color(240, 240, 240));
					tfMenuManage.setForeground(new Color(0, 0, 0));
					tfDashboard.setBackground(new Color(240, 240, 240));
					tfDashboard.setForeground(new Color(0, 0, 0));
					
					tfShow.setText("직원관리");
				}
			});
			tfEmployeeManage.setEditable(false);
			tfEmployeeManage.setText("직원관리");
			tfEmployeeManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfEmployeeManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfEmployeeManage.setColumns(10);
			tfEmployeeManage.setBounds(0, 170, 130, 36);
			tfEmployeeManage.setBorder(null);
		}
		return tfEmployeeManage;
	}

	private JTextField getTfSalesManage() {
		if (tfSalesManage == null) {
			tfSalesManage = new JTextField();
			tfSalesManage.setEditable(false);
			tfSalesManage.setText("매출관리");
			tfSalesManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfSalesManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfSalesManage.setColumns(10);
			tfSalesManage.setBounds(0, 220, 130, 36);
			tfSalesManage.setBorder(null);
		}
		return tfSalesManage;
	}

	private JTextField getTfAdManage() {
		if (tfAdManage == null) {
			tfAdManage = new JTextField();
			tfAdManage.setText("광고관리");
			tfAdManage.setEditable(false);
			tfAdManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfAdManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfAdManage.setColumns(10);
			tfAdManage.setBounds(0, 270, 130, 36);
			tfAdManage.setBorder(null);
		}
		return tfAdManage;
	}

	private JTextField getTfMemberManage() {
		if (tfMemberManage == null) {
			tfMemberManage = new JTextField();
			tfMemberManage.setEditable(false);
			tfMemberManage.setText("회원관리");
			tfMemberManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfMemberManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfMemberManage.setColumns(10);
			tfMemberManage.setBounds(0, 320, 130, 36);
			tfMemberManage.setBorder(null);
		}
		return tfMemberManage;
	}

	private JTextField getTfInventoryManage() {
		if (tfInventoryManage == null) {
			tfInventoryManage = new JTextField();
			tfInventoryManage.setEditable(false);
			tfInventoryManage.setText("재고관리");
			tfInventoryManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfInventoryManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfInventoryManage.setColumns(10);
			tfInventoryManage.setBounds(0, 370, 130, 36);
			tfInventoryManage.setBorder(null);
		}
		return tfInventoryManage;
	}

	private JTextField getTfMenuManage() {
		if (tfMenuManage == null) {
			tfMenuManage = new JTextField();
			tfMenuManage.addMouseListener(new MouseAdapter() {
				@Override
				public void mouseClicked(MouseEvent e) {
					panelAdminMain.setVisible(false);
					adminDashboardPanel.setVisible(false);
					adminStoreManagePanel.setVisible(false);
					adminEmployeeManagePanel.setVisible(false);
			

					adminMenuManagePanel.setVisible(true);
					adminMenuManagePanel.setBounds(167, 55, 1059, 617);
					adminMenuManagePanel.setLayout(null);
					frame.getContentPane().add(adminMenuManagePanel);
					tfSearch.setVisible(false);
					lblSearchBar.setVisible(false);
					tfMenuManage.setBackground(new Color(226, 161, 101));
					tfMenuManage.setForeground(new Color(255, 255, 255));
					tfDashboard.setBackground(new Color(240, 240, 240));
					tfDashboard.setForeground(new Color(0, 0, 0));
					tfEmployeeManage.setBackground(new Color(240, 240, 240));
					tfEmployeeManage.setForeground(new Color(0, 0, 0));
					
					tfShow.setText("메뉴관리");
				}
			});
			tfMenuManage.setEditable(false);
			tfMenuManage.setText("메뉴관리");
			tfMenuManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfMenuManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfMenuManage.setColumns(10);
			tfMenuManage.setBounds(0, 420, 130, 36);
			tfMenuManage.setBorder(null);
		}
		return tfMenuManage;
	}

	private JTextField getTfNoticeManage() {
		if (tfNoticeManage == null) {
			tfNoticeManage = new JTextField();
			tfNoticeManage.setEditable(false);
			tfNoticeManage.setText("공지사항");
			tfNoticeManage.setFont(new Font("굴림", Font.PLAIN, 22));
			tfNoticeManage.setHorizontalAlignment(SwingConstants.CENTER);
			tfNoticeManage.setColumns(10);
			tfNoticeManage.setBounds(0, 470, 130, 36);
			tfNoticeManage.setBorder(null);
		}
		return tfNoticeManage;
	}

	private JLabel getLblBackground() {
		if (lblBackground == null) {
			lblBackground = new JLabel("");
			lblBackground.setBounds(0, 0, 1280, 721);
			lblBackground.setIcon(new ImageIcon(AdminMainFrame.class.getResource("/com/javalec/image/ExecFrameLine.png")));
		}
		return lblBackground;
	}
	private JLabel getLblAlarmIcons() {
		if (lblAlarmIcons == null) {
			lblAlarmIcons = new JLabel("");
			lblAlarmIcons.setIcon(new ImageIcon(AdminMainFrame.class.getResource("/com/javalec/image/AlarmIcons.png")));
			lblAlarmIcons.setBounds(920, 10, 130, 30);
		}
		return lblAlarmIcons;
	}
	private JLabel getLblProfilePicture() {
		if (lblProfilePicture == null) {
			lblProfilePicture = new JLabel("");
			lblProfilePicture.setIcon(new ImageIcon(AdminMainFrame.class.getResource("/com/javalec/image/Kyungho PP.png")));
			lblProfilePicture.setBounds(1060, 4, 34, 41);
		}
		return lblProfilePicture;
	}
	private JTextField getTfAdminName() {
		if (tfAdminName == null) {
			tfAdminName = new JTextField();
			tfAdminName.setFont(new Font("굴림", Font.PLAIN, 18));
			tfAdminName.setText("Kyungho Han");
			tfAdminName.setHorizontalAlignment(SwingConstants.CENTER);
			tfAdminName.setBackground(new Color(240, 240, 240));
			tfAdminName.setBounds(1110, 13, 150, 21);
			tfAdminName.setColumns(10);
			tfAdminName.setBorder(null);
		}
		return tfAdminName;
	}

} // End