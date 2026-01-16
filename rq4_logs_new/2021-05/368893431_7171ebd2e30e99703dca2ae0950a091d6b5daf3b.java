import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Main {

	private JFrame frame;
	private JTextField logicCores;
	private JTextField processor;
	private JTextField op_sys;
	private JTextField architecture;
	private JTextField score;

	public Main() {
		initialize();
	}

	private void initialize() {
		frame = new JFrame();
		frame.getContentPane().setBackground(new Color(25, 122, 120));
		//frame.getContentPane().setForeground(new Color(199, 10, 10));
		frame.setBounds(0, 0, 500, 700);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);

		JLabel team = new JLabel("Brought to you by SSRUM:");
		team.setHorizontalAlignment(SwingConstants.CENTER);
		team.setBounds(100, 510, 300, 14);
		team.setFont(new Font("Times New Roman", Font.BOLD, 15));
		frame.getContentPane().add(team);

		JLabel Mihai = new JLabel("Mermezan Mihai");
		Mihai.setHorizontalAlignment(SwingConstants.CENTER);
		Mihai.setBounds(205, 550, 120, 14);
		frame.getContentPane().add(Mihai);

		JLabel Vlad = new JLabel("Mircea Vlad");
		Vlad.setHorizontalAlignment(SwingConstants.CENTER);
		Vlad.setBounds(205, 570, 120, 14);
		frame.getContentPane().add(Vlad);

		JLabel Dana = new JLabel("Rednic Daniela");
		Dana.setHorizontalAlignment(SwingConstants.CENTER);
		Dana.setBounds(205, 590, 120, 14);
		frame.getContentPane().add(Dana);

		JLabel Alex = new JLabel("Udrea Alexandru");
		Alex.setHorizontalAlignment(SwingConstants.CENTER);
		Alex.setBounds(205, 610, 120, 14);
		frame.getContentPane().add(Alex);

		JSeparator separator1 = new JSeparator();
		separator1.setBounds(40, 0, 421, 5);
		frame.getContentPane().add(separator1);

		JLabel title = new JLabel("BENCHMARK FOR CPU");
		title.setHorizontalAlignment(SwingConstants.CENTER);
		title.setFont(new Font("Times New Roman", Font.BOLD, 18));
		title.setBounds(125, 1, 250, 45);
		frame.getContentPane().add(title);


		JSeparator separator2 = new JSeparator();
		separator2.setBounds(40, 43, 421, 5);
		frame.getContentPane().add(separator2);

		JLabel lblCPU = new JLabel("Processor Type");
		lblCPU.setHorizontalAlignment(SwingConstants.CENTER);
		lblCPU.setFont(new Font("Times New Roman", Font.BOLD, 12));
		lblCPU.setBounds(205, 80, 90, 14);
		frame.getContentPane().add(lblCPU);

		String txtCPU = System.getenv("PROCESSOR_IDENTIFIER");
		processor = new JTextField(txtCPU);
		processor.setEditable(false);
		processor.setHorizontalAlignment(SwingConstants.CENTER);
		processor.setBounds(70, 100, 350, 20);
		frame.getContentPane().add(processor);
		processor.setColumns(10);

		JLabel lblOS = new JLabel("Operating System");
		lblOS.setHorizontalAlignment(SwingConstants.CENTER);
		lblOS.setFont(new Font("Times New Roman", Font.BOLD, 12));
		lblOS.setBounds(195, 125, 110, 14);
		frame.getContentPane().add(lblOS);

		String txtOS = System.getProperty("os.name");
		op_sys = new JTextField(txtOS);
		op_sys.setEditable(false);
		op_sys.setHorizontalAlignment(SwingConstants.CENTER);
		op_sys.setBounds(70, 145, 350, 20);
		frame.getContentPane().add(op_sys);
		op_sys.setColumns(10);
		String arch = System.getenv("PROCESSOR_ARCHITECTURE");

		JLabel lblArch = new JLabel("Architecture");
		lblArch.setHorizontalAlignment(SwingConstants.CENTER);
		lblArch.setFont(new Font("Times New Roman", Font.BOLD, 12));
		lblArch.setBounds(205, 175, 90, 14);
		frame.getContentPane().add(lblArch);

		architecture = new JTextField(arch);
		architecture.setEditable(false);
		architecture.setHorizontalAlignment(SwingConstants.CENTER);
		architecture.setBounds(70, 195, 350, 20);
		frame.getContentPane().add(architecture);
		architecture.setColumns(10);

		JLabel lblCores = new JLabel("Logical Cores: ");
		lblCores.setBounds(205, 225, 90, 14);
		frame.getContentPane().add(lblCores);

		int processors = Runtime.getRuntime().availableProcessors();
		logicCores = new JTextField(processors + "");
		logicCores.setEditable(false);
		logicCores.setHorizontalAlignment(SwingConstants.CENTER);
		logicCores.setBounds(70, 245, 350, 20);
		frame.getContentPane().add(logicCores);
		logicCores.setColumns(10);

		JLabel lblScore = new JLabel("Obtained Score");
		lblScore.setHorizontalAlignment(SwingConstants.CENTER);
		lblScore.setFont(new Font("Times New Roman", Font.BOLD, 12));
		lblScore.setBounds(205, 300, 90, 14);
		frame.getContentPane().add(lblScore);

		JButton btnStart = new JButton("START");
		btnStart.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				/*Testbench master = new Testbench();
				long score1 = master.run();

				score = new JTextField(score1+"");
				score.setHorizontalAlignment(SwingConstants.CENTER);
				score.setBounds(70, 325, 350, 20);
				frame.getContentPane().add(score);
				score.setColumns(10);
				score.setEditable(false);

				String timeStamp = new SimpleDateFormat("yyyy.MM.dd_HH:mm:ss").format(Calendar.getInstance().getTime());
				String str = timeStamp + "\t" + txtCPU + "\t" + arch + "\t" + processors + "\t" + txtOS + "\t" + score1 + "\n";
*/
				try (FileWriter file = new FileWriter("Results.txt")) {
					file.write("Hello, CO Project people!");
							file.flush();

				} catch (IOException o) {
					o.printStackTrace();
				}
			}
		});
		btnStart.setBounds(205, 360, 90, 40);
		frame.getContentPane().add(btnStart);
	}

	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					Main window = new Main();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
}