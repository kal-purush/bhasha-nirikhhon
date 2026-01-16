import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JLabel;

import java.awt.BorderLayout;

import javax.swing.JPanel;

import java.awt.FlowLayout;

import javax.swing.JTextField;
import javax.swing.JTable;

import java.awt.Font;
import java.awt.Color;

import javax.swing.JList;
import javax.swing.JButton;
import javax.swing.JTabbedPane;
import javax.swing.JToolBar;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JScrollBar;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import java.awt.Component;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;


public class GUI {

	private JFrame frame;
	private JTextField link;
	private JLabel lbleingefuegtelinnks;
	private JTextField speicherort;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GUI window = new GUI();
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
	public GUI() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 820, 500);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		JLabel lbllinkeinfuegen = new JLabel("Link einf\u00FCgen:");
		lbllinkeinfuegen.setBounds(12, 49, 90, 30);
		frame.getContentPane().add(lbllinkeinfuegen);
		
		lbleingefuegtelinnks = new JLabel("Links zum herunterladen:");
		lbleingefuegtelinnks.setBounds(12, 157, 155, 30);
		frame.getContentPane().add(lbleingefuegtelinnks);
		
		JLabel lblspeicherort = new JLabel("Speicherort:");
		lblspeicherort.setBounds(12, 10, 90, 30);
		frame.getContentPane().add(lblspeicherort);
		
		JButton btnlinkhinzufuegen = new JButton("Link hinzuf\u00FCgen");
		btnlinkhinzufuegen.setBounds(12, 107, 131, 25);
		frame.getContentPane().add(btnlinkhinzufuegen);
		
		JButton btnherunterladen = new JButton("Herunterladen");
		btnherunterladen.setBounds(12, 383, 131, 25);
		frame.getContentPane().add(btnherunterladen);
		
		JButton btnschliessen = new JButton("Schlie\u00DFen");
		btnschliessen.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				System.exit(0);
			}
		});
		btnschliessen.setBounds(693, 415, 97, 25);
		frame.getContentPane().add(btnschliessen);
		
		JButton btndurchsuchen = new JButton("Durchsuchen");
		btndurchsuchen.setBounds(675, 13, 115, 25);
		frame.getContentPane().add(btndurchsuchen);
		
		JButton btnordneroeffnen = new JButton("Ordner \u00F6ffnen");
		btnordneroeffnen.setBounds(12, 415, 131, 25);
		frame.getContentPane().add(btnordneroeffnen);
		
		link = new JTextField();
		link.setBounds(12, 77, 778, 22);
		frame.getContentPane().add(link);
		link.setColumns(10);
		
		speicherort = new JTextField();
		speicherort.setBounds(103, 13, 560, 25);
		frame.getContentPane().add(speicherort);
		speicherort.setColumns(10);
		
		JList list = new JList();
		list.setBounds(12, 190, 754, 100);
		frame.getContentPane().add(list);
		
		JProgressBar progressBargesamt = new JProgressBar();
		progressBargesamt.setBounds(12, 356, 778, 14);
		frame.getContentPane().add(progressBargesamt);
		
		JProgressBar progressBareinzel = new JProgressBar();
		progressBareinzel.setBounds(12, 318, 778, 14);
		frame.getContentPane().add(progressBareinzel);
		
		JScrollBar scrollBar = new JScrollBar();
		scrollBar.setBounds(769, 190, 21, 100);
		frame.getContentPane().add(scrollBar);
		
		JLabel lblgesamt = new JLabel("Gesamt:");
		lblgesamt.setBounds(12, 340, 56, 16);
		frame.getContentPane().add(lblgesamt);
		
		JLabel lblaktuell = new JLabel("Aktuell:");
		lblaktuell.setBounds(12, 303, 56, 16);
		frame.getContentPane().add(lblaktuell);
	}
	private static void addPopup(Component component, final JPopupMenu popup) {
	}
}