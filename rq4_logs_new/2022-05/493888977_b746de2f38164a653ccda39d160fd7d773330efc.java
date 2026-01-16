package aima.core.ia.agenda_normal;

import java.util.List;
import java.util.Random;

public class Agenda {
	private List<Funcionario> funcionarios;
	private HorarioNormal[][] horarios;
	public int fitness;
	public int DIAS;
	public int HORARIOS;
	
	Agenda(List<Funcionario> funcionarios, int dias, int horarios){
		this.funcionarios = funcionarios;
		this.DIAS = dias;
		this.HORARIOS = horarios;
		this.horarios= new HorarioNormal[DIAS][HORARIOS];
		this.horarios = this.createParents();
	} 
	
	public void printAll() {
		printFuncionarios();
		printHorarios();
	}
	
	private void printFuncionarios() {
		for (Funcionario funcionario : this.funcionarios) {
			System.out.printf("Funcionario: %s\n\tDias: [ ", funcionario.getNome(), funcionario.getHorasDeTrabalho());
			boolean[][] horariosDisponiveis = funcionario.getHorariosDisponiveis(); 
			for (int i = 0 ; i < horariosDisponiveis.length ; i++) {
				for (int j = 0 ; j < horariosDisponiveis[i].length ; j++) {
					if (horariosDisponiveis[i][j]) {
						System.out.printf("dia: %d [%d] ", i+1, j);
					}
				}
			}
			System.out.print("]\n");
		}
	}
	
	private void printHorarios() {
		for (int i = 0 ; i < this.horarios.length ; i++) {
			System.out.printf("Dia %d\n", i+1);
			HorarioNormal[] dia = this.horarios[i];
			for (int j = 0 ; j < HORARIOS ; j++) {
				System.out.print("\t"+(j+1));
				HorarioNormal hr = this.horarios[i][j];
				if (hr != null && hr.getNomes().length > 0) {
					String[] nomesFuncionarios = this.horarios[i][j].getNomes();
					String nomes = "";
					for (String nome : nomesFuncionarios) {
						nomes += " " + nome;
					}
					System.out.println(nomes);	
				}
				else {
					System.out.println(" null");
				}
			}
		}
	}
	
	private HorarioNormal[][] createParents(){
		HorarioNormal[][] horarios = new HorarioNormal[DIAS][HORARIOS];
		Random rand = new Random();
		for (int i = 0; i < funcionarios.size(); i++) {
			Funcionario funcionario = funcionarios.get(i);
			int dia = rand.nextInt(DIAS);
			int horario = rand.nextInt(HORARIOS);
			horarios = this.addFuncionario(horarios, dia, horario, funcionario);
		}
		return horarios;
	}
	
	private HorarioNormal[][] addFuncionario(HorarioNormal[][] horarios, int dia, int horario, Funcionario funcionario){
		if(horarios[dia][horario] == null) {
			horarios[dia][horario] = new HorarioNormal();
		}
		horarios[dia][horario].addFuncionarioSePuder(funcionario);
		return horarios;
	}
	
	public int getFuncionario(String name) {
		for (int i = 0 ; i < this.funcionarios.size() ; i++) {
			Funcionario funcionario = this.funcionarios.get(i);
			if (funcionario.getNome() == name) {
				return i;
			}
		}
		return -1;
	}
	
	public List<Funcionario> getFuncionarios(){
		return this.funcionarios;
	}
	
	public void setHorarios(HorarioNormal[][] horarios) {
		this.horarios = horarios;
	}
	
	public HorarioNormal[][] getHorarios(){
		return this.horarios;
	}
}