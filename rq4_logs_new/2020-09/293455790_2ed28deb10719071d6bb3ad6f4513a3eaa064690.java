package com.xx.about_dialog;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dialog;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GraphicsConfiguration;
import java.awt.Window;

import javax.swing.BorderFactory;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class WaittingDialog extends JDialog{
	private static final long serialVersionUID = -959069046596495131L;
	
	public WaittingDialog() {
		super();
	}

	public WaittingDialog(Dialog owner, boolean title) {
		super(owner, title);
	}

	public WaittingDialog(Dialog owner, String title, boolean modal, GraphicsConfiguration gc) {
		super(owner, title, modal, gc);
	}

	public WaittingDialog(Dialog owner, String title, boolean modal) {
		super(owner, title, modal);
	}

	public WaittingDialog(Dialog owner, String title) {
		super(owner, title);
	}

	public WaittingDialog(Dialog owner) {
		super(owner);
	}

	public WaittingDialog(Frame owner, boolean title) {
		super(owner, title);
	}

	public WaittingDialog(Frame owner, String title, boolean modal, GraphicsConfiguration gc) {
		super(owner, title, modal, gc);
	}

	public WaittingDialog(Frame owner, String title, boolean modal) {
		super(owner, title, modal);
	}

	public WaittingDialog(Frame owner, String title) {
		super(owner, title);
	}

	public WaittingDialog(Frame owner) {
		super(owner);
	}

	public WaittingDialog(Window owner, ModalityType title) {
		super(owner, title);
	}

	public WaittingDialog(Window owner, String title, ModalityType modal, GraphicsConfiguration gc) {
		super(owner, title, modal, gc);
	}

	public WaittingDialog(Window owner, String title, ModalityType modal) {
		super(owner, title, modal);
	}

	public WaittingDialog(Window owner, String title) {
		super(owner, title);
	}

	public WaittingDialog(Window owner) {
		super(owner);
	}
	
	private static final Font normalFont = new Font("宋体", Font.PLAIN, 16);
	private static final int normalFontSize = normalFont.getSize();
	
	public WaittingDialog initDialog(String message) {
		int width = message.length() * 2 * normalFontSize;
		int height = 100;
		
		setSize(width, height);
		setLocationRelativeTo(getOwner());
		setUndecorated(true);
		
		JPanel jpnlBody = new JPanel(new BorderLayout());
		jpnlBody.setBorder(BorderFactory.createLineBorder(Color.gray, 4));
		add(jpnlBody);
		
		JLabel jlblMessage = new JLabel(message, JLabel.CENTER);
		jpnlBody.add(jlblMessage, "Center");
		return this;
	}
	
	public void showDialog() {
		setVisible(true);
	}
	
	public void closeDialog() {
		dispose();
	}
}