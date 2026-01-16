package de.hsbochum.fbg.kswe.aufgabe1;

import java.util.ArrayList;
import java.util.List;

public class TeamMemberLogger {
	
	public static void main(String[] args) {
		TeamMemberLogger tml = new TeamMemberLogger();
		tml.loadTeamMembers();
		tml.doLog();
	}

	private List<TeamMember> members;

	private void loadTeamMembers() {
		this.members = new ArrayList<>();
		
		loadMemberA();
		loadMemberB();
		loadMemberC();
	}

	private void loadMemberC() {
		this.members.add(new TeamMember("Barbara", "Klein"));
	}

	private void loadMemberB() {
		
	}

	private void loadMemberA() {
		
	}

	private void doLog() {
		System.out.println(String.format("%s Mitglieder insgesamt:", this.members.size()));
		this.members.stream().forEach(m -> {
			System.out.println(m.toString());
		});
	}
	
}