package com.the.gui;

import java.awt.Choice;
import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JFrame;

public class MovieApp extends JFrame{
	Choice ch;
	JButton bt;
	public MovieApp() {
		ch = new Choice();
		bt = new JButton("결과보기");
		
		ch.add("para");
		ch.add("starwars");
		ch.add("007");
		setLayout(new FlowLayout());
		add(ch);
		add(bt);
		setVisible(true);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setSize(300,150);
	}
	public static void main(String[] args) {
		new MovieApp();
	}
}