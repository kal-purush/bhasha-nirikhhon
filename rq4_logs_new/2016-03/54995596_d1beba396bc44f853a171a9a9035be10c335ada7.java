package nl.tudelft.goal.thesis;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.text.DefaultCaret;

import eis.eis2java.environment.AbstractEnvironment;
import eis.exceptions.EntityException;
import eis.exceptions.ManagementException;
import eis.exceptions.NoEnvironmentException;
import eis.exceptions.PerceiveException;
import eis.iilang.Action;
import eis.iilang.EnvironmentState;
import eis.iilang.IILObjectVisitor;
import eis.iilang.IILVisitor;
import eis.iilang.Numeral;
import eis.iilang.Parameter;
import eis.iilang.Percept;

/**
 * Simple Hello World environment which launches a single entity upon initialization.
 * 
 * @author Santiago Conde
 */
public class EIS extends AbstractEnvironment {

	private static final long	serialVersionUID	= 1L;
	private JFrame				frame;						// kill resets this to null.

	@Override
	public void init(Map<String, Parameter> parameters) throws ManagementException {
		super.init(parameters);
		createFrame();
		setState(EnvironmentState.PAUSED);

		try {
			registerEntity("entity1", new Entity(frame));
			
		} catch (EntityException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Initialize the "world": a window displaying text and allowing user input. Based on: http://stackoverflow.com/a/15455725
	 */
	public void createFrame() {

		frame = new JFrame("Environment");
		frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
		panel.setOpaque(true);
		final JTextArea textArea = new JTextArea(15, 80);
		textArea.setWrapStyleWord(true);
		textArea.setEditable(false);
		textArea.setFont(Font.getFont(Font.SANS_SERIF));
		JScrollPane scroller = new JScrollPane(textArea);
		scroller.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		scroller.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		JPanel inputpanel = new JPanel();
		inputpanel.setLayout(new FlowLayout());
		final JTextField input = new JTextField(35);
		JButton button = new JButton("Enter");
		final JTextField textField = new JTextField(35);
		textField.setName("TextField");
		textField.setEditable(false);
		textField.setVisible(false);
		button.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				textField.setText(input.getText());
				input.setText("");
			}
		});
		DefaultCaret caret = (DefaultCaret) textArea.getCaret();
		caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
		panel.add(scroller);
		inputpanel.add(input);
		inputpanel.add(button);
		inputpanel.add(textField);
		panel.add(inputpanel);
		frame.getContentPane().add(BorderLayout.CENTER, panel);
		frame.getRootPane().setDefaultButton(button);
		frame.pack();
		frame.setLocationByPlatform(true);
		frame.setVisible(true);
		frame.setResizable(false);
		input.requestFocus();

	}

	@Override
	public void kill() throws ManagementException {
		super.kill();
		frame.setVisible(false);
		frame = null;

	}

	@Override
	public boolean isStateTransitionValid(EnvironmentState oldState, EnvironmentState newState) {
		return true;
	}

	@Override
	protected boolean isSupportedByEnvironment(Action action) {
		return getState() == EnvironmentState.RUNNING;
	}

	@Override
	protected boolean isSupportedByType(Action action, String type) {
		return true;
	}


}