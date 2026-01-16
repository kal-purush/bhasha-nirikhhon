package com.fortappend;

import javax.swing.*;
import java.awt.*;
import java.util.Vector;

class MyList {
	private JFrame frame = new JFrame("hello world");
	private Container cont = frame.getContentPane();
	private JScrollPane jsp1 = null;
	private JScrollPane jsp2 = null;
	private JList list1 = null;
	private JList list2 = null;

	public MyList() {
		this.frame.setLayout(new GridLayout(1, 3));
		String nation[] = { "china", "usa", "japan", "corea"};
		Vector<String> v = new Vector<String>();// 可实现自动增长对象数组

		v.add("hi");
		v.add("you");
		v.add("who");
		v.add("are");

		this.list1 = new JList(nation);
		this.list2 = new JList(v);

		this.list1.setBorder(BorderFactory.createTitledBorder("label_1"));
		this.list2.setBorder(BorderFactory.createTitledBorder("label_2"));

		this.jsp1 = new JScrollPane(this.list1);
		this.jsp2 = new JScrollPane(this.list2);
		
		this.cont.add(jsp1); // 对list1添加滚动条
		this.cont.add(jsp2); // 对list1添加滚动条

		this.frame.setSize(400, 200);
		this.frame.setVisible(true);
		this.frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	}

	public static void main(String args[]) {
		MyList ml = new MyList();
	}
}