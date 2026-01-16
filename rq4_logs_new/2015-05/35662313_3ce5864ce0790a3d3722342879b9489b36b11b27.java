package gui.controller;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import platform.ProgramController;
import gui.view.HighscoreView;

public class HighscoreController implements ActionListener
{
	private HighscoreView highscoreView;
	private ProgramController programController;
	
	public HighscoreController(Object[][] paramHighscoreData, ProgramController paramProgramController)
	{
		this.programController = paramProgramController;
		this.highscoreView = new HighscoreView(this, paramHighscoreData);
	}

	@Override
	public void actionPerformed(ActionEvent e)
	{
		String actionCommand = e.getActionCommand();
		if(actionCommand.startsWith("sort"))
		{
			String filterAttribute = "";
			switch(actionCommand.substring(5))
			{
				case "name":
					filterAttribute = "username";
					break;
				case "kills":
					filterAttribute = "monsterKillCount";
					break;
				case "deaths":
					filterAttribute = "numberOfDeaths";
					break;
				case "gold":
					filterAttribute = "goldEarned";
					break;
				case "time":
					filterAttribute = "timePlayed";
					break;
			}
			this.programController.updateHighscore(filterAttribute);
		}
		else if(actionCommand.equals("exit"))
			this.programController.closeHighscore();
	}
	
	public void updateHighscore(Object[][] paramHighscoreData)
	{
		if(paramHighscoreData != null)
			this.highscoreView.updateJTables(paramHighscoreData);
		else
			JOptionPane.showMessageDialog(null, "Highscore konnte nicht geladen werden!", "Fehler!", JOptionPane.ERROR_MESSAGE);
	}
	
	public HighscoreView getHighscoreView()
	{
		return this.highscoreView;
	}
}