package com.santipingui58.spleef.commands;


import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import com.santipingui58.spleef.game.Game;
import com.santipingui58.spleef.game.Spleef2v2Game;
import com.santipingui58.spleef.managers.GameManager;
import com.santipingui58.spleef.utils.Scoreboard;

public class MatchesCommand implements CommandExecutor{


	
	
	@Override
	public boolean onCommand(CommandSender p, Command cmd, String label, final String[] args) {
		if(cmd.getName().equalsIgnoreCase("matches")){
				p.sendMessage("�6-=-=-=-[�a�lMatches�6]-=-=-=-");
			p.sendMessage("�5Spleef 1vs1:");
			
			for (Game g : GameManager.getManager().getArenasList()) {
				if (g.getType().equalsIgnoreCase("spleef") ) {
					 if (!g.getPlayer1().isEmpty() && !g.getPlayer2().isEmpty()) {
			
					String p1 = g.getPlayer1().get(0).getName();
					String p2 = g.getPlayer2().get(0).getName();
					int puntos1 = g.getPoints1();
					int puntos2 = g.getPoints2();
					int tiempo = g.getTime();
					String map = g.getId();				
					if (GameManager.getManager().isRanked(g)) {
					p.sendMessage("�dRANKED " + "�a" + p1 + " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + p2 + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");
					} else {
						p.sendMessage("�7UNRANKED " + "�a" + p1 + " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + p2 + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");			
					}
				}
					 
			}
			}
			
			p.sendMessage("�5Spleef 2vs2:");
			
			for (Game g : GameManager.getManager().getArenasList()) {
				 if (g.getType().equalsIgnoreCase("spleef2v2")) {
					 if (GameManager.getManager().isStarted(g)) {
						 if (!g.getPlayer1().isEmpty() && !g.getPlayer2().isEmpty()) {
					String teamA = Spleef2v2Game.teamNames(g.getPlayer1());
					String teamB = Spleef2v2Game.teamNames(g.getPlayer2());
					int puntos1 = g.getPoints1();
					int puntos2 = g.getPoints2();
					int tiempo = g.getTime();
					String map = g.getId();		
					
					
					if (GameManager.getManager().isRanked(g)) {
						p.sendMessage("�dRANKED " + "�a" + teamA + " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + teamB + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");
						} else {
							p.sendMessage("�7UNRANKED " + "�a" + teamA+ " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + teamB + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");			
						}
					
						 }
				}
			}
			}
			
			p.sendMessage("�5BuildSpleefPvP 1vs1:");
			for (Game g : GameManager.getManager().getArenasList()) {
				if (g.getType().equalsIgnoreCase("BuildSpleefPvP") ) {
					 if (!g.getPlayer1().isEmpty() && !g.getPlayer2().isEmpty()) {
			
					String p1 = g.getPlayer1().get(0).getName();
					String p2 = g.getPlayer2().get(0).getName();
					int puntos1 = g.getPoints1();
					int puntos2 = g.getPoints2();
					int tiempo = g.getTime();
					String map = g.getId();				
					if (GameManager.getManager().isRanked(g)) {
					p.sendMessage("�dRANKED " + "�a" + p1 + " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + p2 + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");
					} else {
						p.sendMessage("�7UNRANKED " + "�a" + p1 + " �b" + puntos1 + "�7-�b" + puntos2 + "�a " + p2 + " �7[�e" + map + "�7]" + "�7(�6" + Scoreboard.convert(tiempo)+ "�7)");			
					}
				}
					 
			}
			}
			
			
			
		}
		
		return false;
	}
	
}