
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.Dimension;

import java.io.File;
import java.io.FileNotFoundException;

import java.util.Scanner;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;
/***
 * Classe com escutador externo
 */
public class Cadastro extends JFrame {

	//Define componentes da janela.
	private JLabel jLabelNumero;
	private JLabel jLabelNome;
	private JLabel jLabelGrupo;

	protected JTextField jtextNumero;
	protected JTextField jtextNome;
	protected JTextField jtextGrupo;

	private JButton jbuttonBuscar;

	int indice = 0;

	ArrayList <Dados> dados = new ArrayList<Dados>();


	public Cadastro(String titulo)throws FileNotFoundException {
		super(titulo);

		separa();
		setLayout(new FlowLayout(FlowLayout.RIGHT));

		//Inicializa componentes da janela.
		jLabelNumero = new JLabel("Buscar" );
		jLabelNome = new JLabel("Nome" );
		jLabelGrupo = new JLabel("Grupo");

		jtextNumero = new JTextField(dados.get(1).numero+"",10);
		jtextNome = new JTextField(dados.get(1).nome,10);
		jtextGrupo = new JTextField(dados.get(1).grupo+"",10);

		jbuttonBuscar = new JButton("Buscar");
		jbuttonBuscar.setPreferredSize(new Dimension(180,18));

		jtextNome.setEditable(false);
		jtextGrupo.setEditable(false);
		//Adiciona componentes na janela.
		this.add(jLabelNumero);
		this.add(jtextNumero);
		this.add(jbuttonBuscar);

		this.add(jLabelNome);
		this.add(jtextNome);

		this.add(jLabelGrupo);
		this.add(jtextGrupo);



		//Cria o escutador
		Escutador handler = new Escutador();

		//Adiciona o escutador a cada botão.
		jbuttonBuscar.addActionListener(handler);



		//Logica de inicialização aqui.
		// ...



	}
	public void separa()throws FileNotFoundException{
		Scanner arquivo = new Scanner(new File("dados.csv"));

		arquivo.nextLine();
		dados.add(new Dados(0,"null", 0));
		while(arquivo.hasNext()){
			String[] line = arquivo.nextLine().split(",");
			int numero = Integer.parseInt(line[0]);
			String nome = line[1];
			int grupo = Integer.parseInt(line[2]);
			dados.add(new Dados(numero, nome, grupo));
		}

	}
	public void atualiza(){
		jtextNome.setText(dados.get(indice).nome);
		jtextGrupo.setText(dados.get(indice).grupo+"");
	}

	private class Escutador implements ActionListener {

		@Override
		public void actionPerformed(ActionEvent event) {

			indice = Integer.parseInt(jtextNumero.getText());

			if(indice <= 0){
				indice = 1;
				jtextNumero.setText(dados.get(indice).numero+"");
			}
			else if(indice > dados.size()-1){
				indice = 26;
				jtextNumero.setText(dados.get(indice).numero+"");
			}
			atualiza();
		}

	}

	public static void main(String[] args) throws FileNotFoundException{
		Cadastro cadastro = new Cadastro("Cadastro");
		cadastro.pack();
		cadastro.setSize(210,150);
		cadastro.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		cadastro.setVisible(true);
	}

}
class Dados{
	int numero, grupo;
	String nome;
	public Dados(int numero, String nome, int grupo){
		this.numero = numero;
		this.nome = nome;
		this.grupo = grupo;
	}
}

