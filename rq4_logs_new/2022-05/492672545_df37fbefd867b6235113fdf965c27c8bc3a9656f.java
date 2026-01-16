package dvmProject;

import java.awt.EventQueue;

import javax.swing.JFrame;

public class dvm_Main 
{
	private JFrame frame;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) 
	{
		EventQueue.invokeLater(new Runnable() 
		{
			public void run() 
			{
				try 
				{
					dvm_Main window = new dvm_Main();
					window.frame.setVisible(true);
				} 
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public dvm_Main() 
	{
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() 
	{
		frame = new JFrame();
		frame.setTitle("객지방 3조 분산자판기");
		frame.setBounds(100, 50, 600, 750);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}

}