package UserInterfaces;

import ClassesMétiers.*;
import com.sun.media.jfxmediaimpl.platform.java.JavaPlatform;
import com.sun.xml.internal.ws.api.pipe.FiberContextSwitchInterceptor;
import javafx.scene.control.CheckBox;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.reflect.Array;
import java.util.ArrayList;

public class GUI implements Runnable{

	private Manager manager1 = new Manager("Palpatine");
	private ArrayList<Manager> listManager = new ArrayList<>();

	Worker worker1=new Worker("Dark Vador");
	Worker worker2=new Worker("Luc");
	Worker worker3=new Worker("Obi Wan");
	Worker worker4=new Worker("Yoda");
	Worker worker5=new Worker("Leia");
	Worker worker6=new Worker("Jabba");
	Worker worker7=new Worker("Han");
	Worker worker8=new Worker("Chewie");
	ArrayList<Worker> listWorker=new ArrayList<>();


	@Override
	public void run(){


		//Création du premier frame
		JFrame frameLogin = new JFrame("Login");
		frameLogin.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		//Création du panel de Login
		JPanel panelLogin = new JPanel();

		JComboBox comboBoxLogin = new JComboBox();
		listManager.add(manager1);
		listWorker.add(worker1);
		listWorker.add(worker2);
		listWorker.add(worker3);
		listWorker.add(worker4);
		listWorker.add(worker5);
		listWorker.add(worker6);
		listWorker.add(worker7);
		listWorker.add(worker8);

		comboBoxLogin.addItem(manager1.getName());

		for(int i=0;i<listWorker.size();i++){
			comboBoxLogin.addItem(listWorker.get(i).getName());
		}
		panelLogin.add(comboBoxLogin);

		//Création de LOGIN
		JButton buttonLogin = new JButton("Login");
		panelLogin.add(buttonLogin);
		frameLogin.add(panelLogin);
		frameLogin.pack();
		frameLogin.setSize(300,100);
		frameLogin.setLocationRelativeTo(null);
		frameLogin.setVisible(true);


		//Créatio du menu pour le BOSS
		JFrame frameConnectManager = new JFrame("Hello Boss");
		frameConnectManager.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		JPanel panelConnectManager = new JPanel();
		JPanel panelCreateTeam = new JPanel();
		JPanel panelAddSkill= new JPanel();
		JPanel panelShowTeam = new JPanel();

		JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
		split.setTopComponent(panelConnectManager);
		split.setDividerLocation(200);

		JMenuBar menuBoss = new JMenuBar();

		//Céation du frame pour un Worker
		Task task1 = new Task("ranger", worker1);
		Task task2 = new Task("demonter", worker1);
		Task task3 = new Task("tout casser", worker1);
		ArrayList<Task> listTasktest = new ArrayList<>();

		listTasktest.add(task1);
		listTasktest.add(task2);
		listTasktest.add(task3);
		JFrame frameConnectWorker = new JFrame("Hello Worker");
		frameConnectWorker.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		JPanel panelAgendaWorker = new JPanel();
		JButton logoutWorker = new JButton("Logout");
		frameConnectWorker.add(panelAgendaWorker);
		ArrayList<JLabel> listLabel= new ArrayList<>();

		//MANAGE USERS
		JMenu manageUsers = new JMenu("Manage Users");
		JMenuItem showWorker= new JMenuItem("Show workers");
		JMenuItem createTeam= new JMenuItem("Create team");
		JMenuItem addSkill= new JMenuItem("Add a skill to a worker");
		JMenuItem showTeam= new JMenuItem("show team");
		manageUsers.add(showWorker);
		manageUsers.add(createTeam);
		manageUsers.add(showTeam);
		manageUsers.add(addSkill);
		menuBoss.add(manageUsers);


		JComboBox comboBoxWorker = new JComboBox();
		JComboBox comboBoxSkill = new JComboBox();






		//MANAGE RESOURCES
		JMenu manageResources = new JMenu("Manage Resources");
		JMenuItem showResource = new JMenuItem("Show resources");
		JMenuItem addResource = new JMenuItem("Add resources");
		JMenuItem modifyResource = new JMenuItem("Modify resources");
		manageResources.add(addResource);
		manageResources.add(showResource);
		manageResources.add(modifyResource);
		menuBoss.add(manageResources);






		//MANAGE TASKS
		JMenu manageTasks = new JMenu("Manage Tasks");
		JMenuItem addTasks= new JMenuItem("Add tasks");
		JMenuItem showTasks= new JMenuItem("Show tasks");
		JMenuItem modifyTasks= new JMenuItem("Modify tasks");
		manageTasks.add(addTasks);
		manageTasks.add(showTasks);
		manageTasks.add(modifyTasks);


		//AFFECT TASKS
		JMenu affectTasks = new JMenu("Affect Tasks");
		JMenuItem affectTaskItem= new JMenuItem("Affect Tasks");
		affectTasks.add(affectTaskItem);

		JMenu managerPlanning = new JMenu("Planning");
		JMenuItem managerPlanningItem = new JMenuItem("Show the planning");
		managerPlanning.add(managerPlanningItem);

		JMenu back = new JMenu("Connection");
		JMenuItem backItem= new JMenuItem("Logout");
		back.add(backItem);




		menuBoss.add(manageTasks);
		menuBoss.add(affectTasks);
		menuBoss.add(managerPlanning);
		menuBoss.add(back);
		panelConnectManager.add(menuBoss);

		frameConnectManager.add(split);


		ArrayList<JCheckBox> cbgTeam = new ArrayList<>();
		for(int i=0;i<listWorker.size();i++)
		{
			cbgTeam.add(new JCheckBox(listWorker.get(i).getName()));
		}
		JButton buttonAddTeam = new JButton("Add ");


		ArrayList<Team> teamAdd = new ArrayList<>();


		//LOGIN
		buttonLogin.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				String value=comboBoxLogin.getSelectedItem().toString();
				for(int i=0;i<listManager.size();i++)
				{
					if(value.equals(listManager.get(i).getName())){
						frameLogin.setVisible(false);

						frameConnectManager.pack();
						frameConnectManager.setSize(550,200);
						frameConnectManager.setLocationRelativeTo(null);
						frameConnectManager.setVisible(true);
					}
				}
				for(int i=0;i<listWorker.size();i++)
				{
					if(value.equals(listWorker.get(i).getName())){
						JOptionPane.showMessageDialog(null,"Hello "+ listWorker.get(i).getName());
						frameLogin.setVisible(false);

						String task = "";
						for(int j = 0; j<listTasktest.size();j++){
							if(value.equals(listTasktest.get(j).getWorker().getName())){
								listLabel.add(new JLabel(listTasktest.get(j).toString()));
							}
						}
						for(int k=0; k<listLabel.size();k++){
							panelAgendaWorker.add(listLabel.get(k));
						}
						panelAgendaWorker.add(logoutWorker);
						frameConnectWorker.pack();
						frameConnectWorker.setSize(800,400);
						frameConnectWorker.setLocationRelativeTo(null);
						frameConnectWorker.setVisible(true);
					}
				}
			}
		});

		logoutWorker.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				listLabel.removeAll(listLabel);
				panelAgendaWorker.removeAll();
				panelAgendaWorker.revalidate();
				panelAgendaWorker.repaint();
				frameLogin.setVisible(true);
				frameConnectWorker.setVisible(false);
			}
		});


		//CREER UNE EQUIPE
		createTeam.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				split.setBottomComponent(panelCreateTeam);


				for(int i=0;i<cbgTeam.size();i++)
				{
					panelCreateTeam.add(cbgTeam.get(i));
				}

				panelCreateTeam.add(buttonAddTeam);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});


		buttonAddTeam.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {

				ArrayList<Worker> teamWorker = new ArrayList<>();

				for(int i=0;i<cbgTeam.size();i++){
					if(cbgTeam.get(i).isSelected()){
						teamWorker.add(listWorker.get(i));
					}
				}
				for(int i=0;i<cbgTeam.size();i++){
					cbgTeam.get(i).setSelected(false);
				}

				teamAdd.add(new Team(teamWorker));

			}
		});


		//VOIR UNE EQUIPE
		JButton buttonShowTeam = new JButton("Show");
		JComboBox comboBoxShowTeam = new JComboBox();
		showTeam.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxShowTeam.removeAllItems();
				split.setBottomComponent(panelShowTeam);

				for(int i=0;i<teamAdd.size();i++) {
					comboBoxShowTeam.addItem(teamAdd.get(i).getTeamName());
				}

				panelShowTeam.add(comboBoxShowTeam);
				panelShowTeam.add(buttonShowTeam);
				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});

		buttonShowTeam.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				for(int i=0;i<teamAdd.size();i++){
					if(comboBoxShowTeam.getSelectedItem().toString().equals(teamAdd.get(i).getTeamName())){
						String test = new String();
						test=teamAdd.get(i).toString();
						JOptionPane.showMessageDialog(null,test);
					}
				}
			}
		});




		//AJOUTER UN SKILL
		JTextField fieldSkill = new JTextField(10);
		JButton buttonAddSkil = new JButton("Add");
		addSkill.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxWorker.removeAllItems();

				for(int i=0;i<listWorker.size();i++){
					comboBoxWorker.addItem(listWorker.get(i).getName());
				}

				panelAddSkill.add(comboBoxWorker);
				panelAddSkill.add(fieldSkill);
				panelAddSkill.add(buttonAddSkil);
				split.setBottomComponent(panelAddSkill);

				frameConnectManager.add(split);

				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);

			}
		});


		buttonAddSkil.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				for(int i=0;i<listWorker.size();i++){
					if(listWorker.get(i).getName().equals(comboBoxWorker.getSelectedItem())){
						Skill listSkill = new Skill(fieldSkill.getText().toString());
						listWorker.get(i).setSkill(listSkill);
					}
				}
				fieldSkill.setText(null);
				JOptionPane.showMessageDialog(null,"Skill add");
			}
		});


		//VOIR WORKER
		JPanel panelShowWorker = new JPanel();
		JButton buttonShowWorker = new JButton("Show me what you got");
		showWorker.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxWorker.removeAllItems();
				split.setBottomComponent(panelShowWorker);

				for(int i=0;i<listWorker.size();i++){
					comboBoxWorker.addItem(listWorker.get(i).getName());
				}
				panelShowWorker.add(comboBoxWorker);
				panelShowWorker.add(buttonShowWorker);
				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);

			}
		});

		buttonShowWorker.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {

				for(int i=0;i<listWorker.size();i++){
					if(comboBoxWorker.getSelectedItem().toString().equals(listWorker.get(i).getName())){
						String test = new String();
						test=listWorker.get(i).toString();
						JOptionPane.showMessageDialog(null,test);
					}
				}
			}
		});



		//AJOUTER UNE RESSOURCE
		JPanel panelAddResource = new JPanel();
		ArrayList<BasicResource> listResource = new ArrayList<>();
		JTextField fieldAddResource = new JTextField(10);
		JButton buttonAddResource = new JButton("Add");

		addResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				fieldAddResource.setText(null);
				split.setBottomComponent(panelAddResource);
				panelAddResource.add(fieldAddResource);
				panelAddResource.add(buttonAddResource);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});

		buttonAddResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				listResource.add(new BasicResource(fieldAddResource.getText().toString()));
				JOptionPane.showMessageDialog(null,"Resource add");
				fieldAddResource.setText(null);
			}
		});



		//VOIR UNE RESSOURCE
		JPanel panelShowResource = new JPanel();
		JComboBox comboBoxShowResource = new JComboBox();
		JButton buttonShowResource = new JButton("Show");
		showResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxShowResource.removeAllItems();
				split.setBottomComponent(panelShowResource);

				for(int i=0;i<listResource.size();i++)
				{
					comboBoxShowResource.addItem(listResource.get(i).getName());
				}

				panelShowResource.add(comboBoxShowResource);
				panelShowResource.add(buttonShowResource);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);

			}
		});

		buttonShowResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {

				for(int i=0;i<listResource.size();i++){
					if(comboBoxShowResource.getSelectedItem().toString().equals(listResource.get(i).getName())){
						String test = new String();
						test=listResource.get(i).toString();
						JOptionPane.showMessageDialog(null,test);
					}
				}
			}
		});



		//MODIFIER UNE RESSOURCE
		JPanel panelModifyResource = new JPanel();
		JButton buttonModifyResource = new JButton("Modify");
		JButton buttonInputResource = new JButton("Input");
		JTextField fieldResourceName = new JTextField(8);
		JRadioButton radioResourceStatus = new JRadioButton("Status");
		JTextField fieldResourceState = new JTextField(3);
		modifyResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxShowResource.removeAllItems();
				fieldResourceName.setText(null);
				radioResourceStatus.setSelected(false);
				fieldResourceState.setText(null);
				split.setBottomComponent(panelModifyResource);

				for(int i=0;i<listResource.size();i++)
				{
					comboBoxShowResource.addItem(listResource.get(i).getName());
				}

				panelModifyResource.add(comboBoxShowResource);
				panelModifyResource.add(fieldResourceName);
				panelModifyResource.add(radioResourceStatus);
				panelModifyResource.add(fieldResourceState);
				panelModifyResource.add(buttonInputResource);
				panelModifyResource.add(buttonModifyResource);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);

			}
		});

		buttonInputResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				fieldResourceName.setText(null);
				radioResourceStatus.setSelected(false);
				fieldResourceState.setText(null);

				for(int i=0;i<listResource.size();i++)
				{
					if(comboBoxShowResource.getSelectedItem().toString().equals(listResource.get(i).getName())){
						fieldResourceName.setText(listResource.get(i).getName());
						radioResourceStatus.setSelected(listResource.get(i).getStatus());
						fieldResourceState.setText(String.valueOf(listResource.get(i).getState()));

					}
				}
			}
		});

		buttonModifyResource.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				for(int i=0;i<listResource.size();i++)
				{
					if(comboBoxShowResource.getSelectedItem().toString().equals(listResource.get(i).getName()))
					{
						listResource.get(i).setName(fieldResourceName.getText());
						if(radioResourceStatus.isSelected())
						{
							listResource.get(i).setStatus(true);
						}else
						{
							listResource.get(i).setStatus(false);
						}

						float statetmp = Float.parseFloat(fieldResourceState.getText());
						listResource.get(i).setState(statetmp);
					}

				}

				comboBoxShowResource.removeAllItems();
				for(int i=0;i<listResource.size();i++)
				{
					comboBoxShowResource.addItem(listResource.get(i).getName());
				}


				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});



		//AJOUTER UNE TACHE
		JPanel panelAddTask = new JPanel();
		ArrayList<Task> listTask = new ArrayList<>();
		ArrayList<AgendaEntry> listAgendaEntry = new ArrayList<>();
		JTextField fieldAddTaskName = new JTextField(10);
		JComboBox comboBoxTaskWorker = new JComboBox();
		JButton buttonAddTask = new JButton("Add");

		addTasks.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				split.setBottomComponent(panelAddTask);
				panelAddTask.add(fieldAddTaskName);
				panelAddTask.add(buttonAddTask);
			}
		});

		buttonAddTask.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				listAgendaEntry.add(new AgendaEntry());
				listTask.add(new Task(fieldAddTaskName.getText(),new AgendaEntry()));
				JOptionPane.showMessageDialog(null,"Task add");
				fieldAddTaskName.setText(null);
			}
		});



		//VOIR UNE TACHE
		JPanel panelShowTask = new JPanel();
		JComboBox comboBoxShowTask = new JComboBox();
		JButton buttonShowTask = new JButton("Show");
		showTasks.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				comboBoxShowTask.removeAllItems();
				split.setBottomComponent(panelShowTask);

				for(int i=0;i<listTask.size();i++)
				{
					comboBoxShowTask.addItem(listTask.get(i).getName());
				}


				panelShowTask.add(comboBoxShowTask);
				panelShowTask.add(buttonShowTask);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});

		buttonShowTask.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				for(int i=0;i<listTask.size();i++){
					if(comboBoxShowTask.getSelectedItem().toString().equals(listTask.get(i).getName().toString())){
						JOptionPane.showMessageDialog(null,listTask.get(i).toString());
					}
				}
			}
		});


		//MODIFIER UNE TACHE
		JPanel panelModifyTask = new JPanel();
		JComboBox comboBoxModifyTask = new JComboBox();
		JButton buttonModifyTask = new JButton("Modify");
		JButton buttonInputTask = new JButton("Input");
		JTextField fieldTaskName = new JTextField(8);
		JTextField fieldTaskProgress = new JTextField(3);
		JTextField fieldAgendaStart = new JTextField(3);
		JTextField fieldAgendaEnd = new JTextField(3);
		modifyTasks.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				split.setBottomComponent(panelModifyTask);

				for(int i=0;i<listTask.size();i++)
				{
					comboBoxModifyTask.addItem(listTask.get(i).getName());
				}

				panelModifyTask.add(comboBoxModifyTask);
				panelModifyTask.add(fieldTaskName);
				panelModifyTask.add(fieldTaskProgress);
				panelModifyTask.add(fieldAgendaStart);
				panelModifyTask.add(fieldAgendaEnd);
				panelModifyTask.add(buttonInputTask);
				panelModifyTask.add(buttonModifyTask);

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);

			}
		});

		buttonInputTask.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				fieldTaskName.setText(null);
				fieldTaskProgress.setText(null);
				fieldAgendaStart.setText(null);
				fieldAgendaEnd.setText(null);

				for(int i=0;i<listTask.size();i++)
				{
					if(comboBoxModifyTask.getSelectedItem().toString().equals(listTask.get(i).getName())){
						fieldTaskName.setText(listTask.get(i).getName());
						fieldTaskProgress.setText(String.valueOf(listTask.get(i).getProgress()));
						fieldAgendaStart.setText(String.valueOf(listTask.get(i).getAgendaEntry().getStartTime()));
						fieldAgendaEnd.setText(String.valueOf(listTask.get(i).getAgendaEntry().getEndTime()));
					}
				}
			}
		});

		buttonModifyTask.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				for(int i=0;i<listTask.size();i++)
				{
					if(comboBoxModifyTask.getSelectedItem().toString().equals(listTask.get(i).getName()))
					{
						JOptionPane.showMessageDialog(null,"you are in");
						listTask.get(i).setName(fieldTaskName.getText());
						listTask.get(i).setProgress(Integer.parseInt(fieldTaskProgress.getText()));
						listAgendaEntry.get(i).setStartTime(Integer.parseInt(fieldAgendaStart.getText()));
						listAgendaEntry.get(i).setEndTime(Integer.parseInt(fieldAgendaEnd.getText()));
						listTask.get(i).setAgendaEntry(listAgendaEntry.get(i));
					}

				}

				comboBoxModifyTask.removeAllItems();
				for(int i=0;i<listTask.size();i++)
				{
					comboBoxModifyTask.addItem(listTask.get(i).getName());
				}


				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});


 		//VOIR LE PLANNING
		JPanel panelShowPlanning = new JPanel();
		ArrayList<JLabel> labelPlanning = new ArrayList<>();
		managerPlanningItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				labelPlanning.removeAll(labelPlanning);
				panelShowPlanning.removeAll();
				panelShowPlanning.revalidate();
				panelShowPlanning.repaint();
				split.setBottomComponent(panelShowPlanning);
				for(int i=0;i<listTask.size();i++){
					labelPlanning.add(new JLabel(listTask.get(i).toString()));
				}
				for(int i=0; i<listTasktest.size();i++){
					labelPlanning.add(new JLabel(listTasktest.get(i).toString()));
				}

				for(int i=0;i<labelPlanning.size();i++){
					panelShowPlanning.add(labelPlanning.get(i));

				}

				frameConnectManager.add(split);
				frameConnectManager.pack();
				frameConnectManager.setSize(550,200);
				frameConnectManager.setLocationRelativeTo(null);
				frameConnectManager.setVisible(true);
			}
		});

		backItem.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				frameConnectManager.setVisible(false);
				frameLogin.setVisible(true);
			}
		});



	}

	public static void main(String[] args) {
		SwingUtilities.invokeLater(new GUI());
	}

}