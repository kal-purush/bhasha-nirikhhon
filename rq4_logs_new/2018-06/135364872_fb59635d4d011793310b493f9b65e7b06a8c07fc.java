package servidor.app.frame;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import org.json.JSONException;
import org.json.JSONObject;

import javax.swing.JLabel;
import javax.swing.JButton;
import javax.swing.JTextField;
import javax.swing.JTextArea;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import cliente.app.regras.OnlinesTableModel;
import cliente.app.util.Cliente;

import javax.swing.ListSelectionModel;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.awt.event.ActionEvent;
import java.awt.Font;
import java.awt.Color;

public class ServerGUI extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField txtPorta;
	private JLabel lblIpNum;
	private JButton btnIniciar;
	private JButton btnFechar;
	private JTextArea txtrStatus;
	private JLabel lblStatusJogo;
	private JLabel lblMestre;

	private OnlinesTableModel tableModel;
	private ServidorService servidor;
	private Thread t;

	// Server
	private int porta;
	private ServerSocket serverSocket;
	private Socket socket;
	private HashMap<String, Cliente> mapOnlines = new HashMap<>();
	private ArrayList<Cliente> prontos = new ArrayList<>();
	private ArrayList<Cliente> sorteio = new ArrayList<>();
	private ArrayList<Cliente> ordem;
	private HashMap<String, JSONObject> mapPacotes = new HashMap<>();
	private boolean iniciado = false;
	private boolean acertaram = false;
	private String palavra;
	private int vez = 1;
	private boolean stopped = false;
	private ArrayList<String> letrasChutadas = new ArrayList<>();

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					ServerGUI frame = new ServerGUI();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public ServerGUI() {
		setTitle("Server Jogo da Forca");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 650, 550);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(null);

		JPanel panelStatus = new JPanel();
		panelStatus.setBounds(5, 156, 450, 350);
		contentPane.add(panelStatus);
		panelStatus.setLayout(null);

		JScrollPane scrollPane = new JScrollPane();
		scrollPane.setBounds(5, 10, 435, 330);
		panelStatus.add(scrollPane);

		txtrStatus = new JTextArea();
		txtrStatus.setEditable(false);
		txtrStatus.setLineWrap(true);
		txtrStatus.setWrapStyleWord(true);
		scrollPane.setViewportView(txtrStatus);

		JPanel panelClientes = new JPanel();
		panelClientes.setBounds(465, 156, 170, 350);
		contentPane.add(panelClientes);
		panelClientes.setLayout(null);

		tableModel = new OnlinesTableModel();
		tableModel.limpar();
		JTable tableOnlines = new JTable(tableModel);
		tableOnlines.setShowVerticalLines(false);
		tableOnlines.setShowGrid(false);
		tableOnlines.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		tableOnlines.setCellSelectionEnabled(true);
		tableOnlines.setBounds(5, 10, 160, 330);
		panelClientes.add(tableOnlines);

		JPanel panelConexao = new JPanel();
		panelConexao.setBounds(5, 0, 630, 85);
		contentPane.add(panelConexao);
		panelConexao.setLayout(null);

		JLabel lblIp = new JLabel("IP:");
		lblIp.setBounds(12, 12, 70, 15);
		panelConexao.add(lblIp);

		JLabel lblPorta = new JLabel("Porta:");
		lblPorta.setBounds(210, 12, 70, 15);
		panelConexao.add(lblPorta);

		btnIniciar = new JButton("Iniciar");
		btnIniciar.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				iniciaServidor();
			}
		});
		btnIniciar.setBounds(353, 30, 120, 30);
		panelConexao.add(btnIniciar);

		lblIpNum = new JLabel("");
		lblIpNum.setBounds(12, 30, 175, 20);
		panelConexao.add(lblIpNum);

		txtPorta = new JTextField();
		txtPorta.setText("20000");
		txtPorta.setBounds(205, 30, 115, 30);
		panelConexao.add(txtPorta);
		txtPorta.setColumns(10);

		btnFechar = new JButton("Fechar");
		btnFechar.setEnabled(false);
		btnFechar.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				fechaServidor();
			}
		});
		btnFechar.setBounds(500, 30, 120, 30);
		panelConexao.add(btnFechar);

		JPanel panelJogo = new JPanel();
		panelJogo.setBounds(5, 88, 630, 65);
		contentPane.add(panelJogo);
		panelJogo.setLayout(null);

		lblMestre = new JLabel("Mestre:");
		lblMestre.setBounds(12, 12, 300, 15);
		panelJogo.add(lblMestre);

		lblStatusJogo = new JLabel("Jogo não iniciado");
		lblStatusJogo.setForeground(Color.RED);
		lblStatusJogo.setFont(new Font("Dialog", Font.BOLD, 16));
		lblStatusJogo.setBounds(411, 12, 207, 20);
		panelJogo.add(lblStatusJogo);

		servidor = null;
		try {
			ipMaquina();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void iniciaServidor() {
		if (!txtPorta.getText().isEmpty() && servidor == null) {
			porta = Integer.parseInt(txtPorta.getText());
			servidor = new ServidorService();
			t = new Thread(servidor);
			t.setName("Server Service");
			t.start();
			btnIniciar.setEnabled(false);
			btnFechar.setEnabled(true);
			txtPorta.setEditable(false);
		}
	}

	private void fechaServidor() {
		if (servidor == null)
			return;
		servidor.stopServer();
		servidor = null;
		btnIniciar.setEnabled(true);
		btnFechar.setEnabled(false);
		txtPorta.setEditable(true);
		//txtrStatus.setText("");
	}

	private void ipMaquina() throws SocketException {
		Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
		while (e.hasMoreElements()) {
			NetworkInterface i = e.nextElement();
			Enumeration<InetAddress> ds = i.getInetAddresses();
			while (ds.hasMoreElements()) {
				InetAddress myself = ds.nextElement();
				if (!myself.isLoopbackAddress() && myself instanceof Inet4Address) {
					lblIpNum.setText("<html>" + i.getName() + ": " + myself.getHostAddress() + "<br></html>");
					// System.out.println("IP: " + myself.getHostAddress());
				}
			}
		}
	}

	private void adicionaStatus(String s) {
		s = s+"\n";
		txtrStatus.append(s);
		//txtrStatus.requestFocus();
		txtrStatus.setCaretPosition(txtrStatus.getText().length());
	}

	private void atualizaOnlines(Set<String> setNames) {
		ArrayList<String> onlines = new ArrayList<>(setNames);
		tableModel.limpar();
		tableModel.addListaClientes(onlines);
	}

	// Server Class
	private class ServidorService implements Runnable {

		public ServidorService() {
			// TODO Auto-generated constructor stub
			stopped = false;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				serverSocket = new ServerSocket(porta);
				while (!stopped) {
					if (!serverSocket.isClosed()) {
						socket = serverSocket.accept();
						new Thread(new ListenerSocket(socket)).start();
					}
				}
			} catch (SocketException e) {
				// TODO: handle exception
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void stopServer() {
			stopped = true;
		}
	}

	private class ListenerSocket implements Runnable {

		private PrintWriter output;
		private BufferedReader input;
		private Socket sock;
		private Cliente cliente;

		public ListenerSocket(Socket s) {
			try {
				sock = s;
				this.output = new PrintWriter(s.getOutputStream(), true);
				this.input = new BufferedReader(new InputStreamReader(s.getInputStream()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {

			JSONObject jsonObject;
			try {
				String s;
				while ((s = input.readLine()) != null) {
					jsonObject = new JSONObject(s);
					if (cliente != null) {
						// System.out.println("Recebeu de " + cliente.getNome() + ": " +
						// jsonObject.toString());
						adicionaStatus("Recebeu de " + cliente.getNome() + ": " + jsonObject.toString());
						cliente.setTempoUltimoPacote(System.currentTimeMillis());
					} else
						/*
						 * System.out.println("Recebeu de " + sock.getInetAddress() + ":" +
						 * sock.getPort() + " :" + jsonObject.toString());
						 */
						adicionaStatus("Recebeu de " + sock.getInetAddress() + ":" + sock.getPort() + " :"
								+ jsonObject.toString());
					switch (jsonObject.getInt("id")) {
					case 1:
						boolean isConnect = connect(jsonObject, output);
						if (isConnect) {
							cliente = new Cliente(jsonObject.getString("nome"), output, sock.getInetAddress(),
									sock.getPort());
							cliente.setTempoUltimoPacote(System.currentTimeMillis());
							mapOnlines.put(jsonObject.getString("nome"), cliente);
							sendOnlines();
						}
						break;
					case 4:
						disconnect(cliente);
						break;
					case 5:
						chatAll(jsonObject, cliente);
						break;
					case 6:
						chatPrivate(jsonObject, cliente);
						break;
					case 8:
						prontoJogo(cliente);
						break;
					case 11:
						palavraEscolhida(jsonObject, cliente);
						break;
					case 13:
						confereLetra(jsonObject, cliente);
						break;
					case 16:
						chutaPalavra(jsonObject, cliente);
						break;
					case 19:
						palavraVerificada(jsonObject, cliente);
						break;
					}
					// System.out.println("passou laço");
				}
				// System.out.println("passou n laço");
			} catch (IOException e) {
				try {
					disconnect(cliente);
				} catch (JSONException e1) {
					e1.printStackTrace();
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}

		private boolean connect(JSONObject json, PrintWriter output) throws JSONException, IOException {
			JSONObject jsonObject = new JSONObject();
			if (mapOnlines.size() < 6 && !mapOnlines.containsKey(json.getString("nome")) && !iniciado) {
				jsonObject.put("id", 2);
				jsonObject.put("conectou", true);
				// System.out.println("Enviou para " + json.getString("nome") + ": " +
				// jsonObject.toString());
				adicionaStatus("Enviou para " + json.getString("nome") + ": " + jsonObject.toString());
				output.println(jsonObject.toString());
				return true;
			} else {
				jsonObject.put("id", 2);
				jsonObject.put("conectou", false);
				if (mapOnlines.size() >= 5)
					jsonObject.put("motivo", "Numero de clientes excedido");
				else if (mapOnlines.containsKey(json.getString("nome")))
					jsonObject.put("motivo", "Nome já existente");
				else
					jsonObject.put("motivo", "Jogo já iniciado");
				// System.out.println("Enviou para " + json.getString("nome") + ": " +
				// jsonObject.toString());
				adicionaStatus("Enviou para " + json.getString("nome") + ": " + jsonObject.toString());
				output.println(jsonObject.toString());
				return false;
			}
		}

		private void disconnect(Cliente cliente) throws JSONException {
			HashMap<String, Cliente> novos = new HashMap<>();
			for (Map.Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				if (!kv.getValue().getOutput().equals(output))
					novos.put(kv.getKey(), kv.getValue());
				else {
					if (iniciado) {
						ordem.remove(cliente);
						sorteio.remove(cliente);
					}
				}
			}
			prontos.remove(cliente);
			if (iniciado) {
				desconexao(cliente);
			}
			mapOnlines = novos;

			if (mapOnlines.size() < 2) {
				iniciado = false;
				lblStatusJogo = new JLabel("Jogo não iniciado");
				lblStatusJogo.setForeground(Color.RED);
				lblMestre.setText("Mestre: ");
			}

			sendOnlines();
		}

		private void send(JSONObject json, Cliente cliente) throws IOException {
			// System.out.println("Enviou para " + cliente.getNome() + ": " +
			// json.toString());
			adicionaStatus("Enviou para " + cliente.getNome() + ": " + json.toString());
			cliente.getOutput().println(json.toString());
		}

		private void sendAll(JSONObject json, Cliente cliente) throws JSONException {
			for (Map.Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				if (!kv.getValue().equals(cliente)) {
					adicionaStatus("Enviou para " + kv.getKey() + ": " + json.toString());
					// System.out.println("Enviou para " + kv.getKey() + ": " + json.toString());
					kv.getValue().getOutput().println(json.toString());
				}
			}
		}

		private void sendOnlines() throws JSONException {
			Set<String> setNames = new HashSet<String>();
			for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				setNames.add(kv.getKey());
			}

			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 3);
			jsonObject.put("qtdClientes", mapOnlines.size());
			jsonObject.put("nome", setNames);
			for (Map.Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				// System.out.println("Enviou para " + kv.getKey() + ": " +
				// jsonObject.toString());
				adicionaStatus("Enviou para " + kv.getKey() + ": " + jsonObject.toString());
				kv.getValue().getOutput().println(jsonObject.toString());

			}
			atualizaOnlines(setNames);
		}

		private void prontoJogo(Cliente cliente) throws JSONException {
			cliente.setPronto(true);
			prontos.add(cliente);
			iniciaJogo();
		}

		private void iniciaJogo() throws JSONException {
			boolean key;
			if (prontos.size() == mapOnlines.size() && prontos.size() > 1) {
				key = true;
			} else {
				key = false;
			}

			if (key) {
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("id", 9);
				// System.out.println("todos prontos, inicia jogo");
				adicionaStatus("Todos prontos, inicia jogo");
				lblStatusJogo.setText("Jogo iniciado");
				lblStatusJogo.setForeground(Color.BLUE);
				sendAll(jsonObject, null);
				iniciado = true;
				sorteio();
			}
		}

		private void sorteio() throws JSONException {
			ArrayList<Cliente> s = new ArrayList<Cliente>(mapOnlines.values());
			Random r = new Random();
			ordem = new ArrayList<>();

			while (ordem.size() < mapOnlines.size()) {
				int numSorteado = r.nextInt(mapOnlines.size());
				if (!ordem.contains(s.get(numSorteado))) {
					s.get(numSorteado).setOrdem(numSorteado);
					s.get(numSorteado).setErros(6 / mapOnlines.size());
					ordem.add(s.get(numSorteado));
				}
			}
			for (Cliente cliente : ordem) {
				// System.out.println(cliente.getNome() + " " + cliente.getOrdem());
				sorteio.add(cliente);
			}
			// System.out.println("Mestre: " + ordem.get(0).getNome());
			adicionaStatus("Mestre: " + ordem.get(0).getNome());
			lblMestre.setText("Mestre: " + ordem.get(0).getNome());
			selecionaMestre(ordem.get(0));
		}

		private void selecionaMestre(Cliente cliente) throws JSONException {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 10);

			try {
				send(jsonObject, cliente);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private ArrayList<String> enviaVez(Cliente cliente) {
			ArrayList<String> setNames = new ArrayList<String>();
			for (int i = 0; i < ordem.size(); i++) {
				if (!ordem.get(i).equals(cliente))
					setNames.add(ordem.get(i).getNome());
			}
			return setNames;
		}

		private void palavraEscolhida(JSONObject json, Cliente cliente) throws JSONException {
			palavra = json.getString("palavra");
			acertaram = false;
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 12);
			jsonObject.put("TamanhoPalavra", json.getString("palavra").length());
			jsonObject.put("vez", enviaVez(cliente));

			sendAll(jsonObject, null);

			enviaVez();

		}

		private void chatAll(JSONObject json, Cliente cliente) throws JSONException {
			JSONObject jsonObject = new JSONObject();
			for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				if (kv.getValue().equals(cliente))
					jsonObject.put("emissor", kv.getKey());
			}

			jsonObject.put("id", 7);
			jsonObject.put("broadcast", true);
			jsonObject.put("mensagem", json.get("mensagem"));

			sendAll(jsonObject, null);
		}

		private void chatPrivate(JSONObject json, Cliente cliente) throws JSONException {
			JSONObject jsonObject = new JSONObject();
			for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				if (kv.getValue().equals(cliente))
					jsonObject.put("emissor", kv.getKey());
			}

			jsonObject.put("id", 7);
			jsonObject.put("broadcast", false);
			jsonObject.put("mensagem", json.get("mensagem"));

			try {
				send(jsonObject, mapOnlines.get(json.getString("destinatario")));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void desconexao(Cliente cliente) throws JSONException {
			if (!ordem.isEmpty()) {
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("id", 21);
				jsonObject.put("vez", enviaVez(ordem.get(0)));

				sendAll(jsonObject, cliente);
			}
		}

		private String indicesLetra(String letra) {
			String p = new String();
			for (int i = 0; i < palavra.length(); i++) {
				if (palavra.charAt(i) == letra.charAt(0))
					p = p + letra.charAt(0);
				else
					p = p + "*";
			}
			return p;
		}

		private void confereLetra(JSONObject json, Cliente cliente) throws JSONException {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 14);
			jsonObject.put("letra", json.getString("letra"));
			if (!letrasChutadas.contains(json.getString("letra")) && palavra.indexOf(json.getString("letra").charAt(0)) != -1) {
				jsonObject.put("correto", true);
				jsonObject.put("palavra", indicesLetra(json.getString("letra")));
				letrasChutadas.add(json.getString("letra"));
			} else {
				jsonObject.put("correto", false);
				jsonObject.put("palavra", indicesLetra(json.getString("letra")));
				cliente.setErros(cliente.getErros() - 1);
				if (cliente.getErros() == 0) {
					ordem.remove(cliente);
					desconexao(cliente);
				}
			}

			sendAll(jsonObject, null);

			if (ordem.size() < 2)
				verificaPalavra();
			else {
				vez++;
				if (vez >= ordem.size())
					vez = 1;
				enviaVez();
			}

		}

		private void chutaPalavra(JSONObject json, Cliente cliente) throws JSONException {
			if (json.getString("palavra").equalsIgnoreCase(palavra)) {
				acertaram = true;
				cliente.setAcertou(true);
				verificaPalavra();
			} else {
				JSONObject jsonObject = new JSONObject();
				ordem.remove(cliente);
				jsonObject.put("id", 17);
				jsonObject.put("palavra", json.getString("palavra"));
				if (ordem.size() < 2)
					verificaPalavra();
				else {
					jsonObject.put("vez", enviaVez(ordem.get(0)));

					sendAll(jsonObject, null);
				}
			}

		}

		private void verificaPalavra() throws JSONException {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 18);
			jsonObject.put("palavra", palavra);
			jsonObject.put("nome", ordem.get(0).getNome());

			sendAll(jsonObject, ordem.get(0));
		}

		private void palavraVerificada(JSONObject json, Cliente cliente) throws JSONException {
			boolean key = true;
			mapPacotes.put(cliente.getNome(), json);
			if (mapPacotes.size() >= (mapOnlines.size() - 1)) {
				for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
					if (!mapPacotes.containsKey(kv.getKey()) && !kv.getValue().equals(ordem.get(0))) {
						key = false;
					}
				}
				if (key) {
					// Seta opiniao da palavra
					for (Entry<String, JSONObject> kv : mapPacotes.entrySet()) {
						mapOnlines.get(kv.getKey()).setAceita(kv.getValue().getBoolean("verificado"));
					}
					// Verifica qtde de aceito
					int ac = 0;
					for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
						if (kv.getValue().isAceita())
							ac++;
					}
					// Verifica se qtde é maior
					boolean palavraAc = false;
					if ((mapPacotes.size() - ac) <= ac)
						palavraAc = true;

					// Limpa mapa de pacotes
					mapPacotes.clear();

					// Se a palavra for aceita, fa Contagem de pontos
					if (palavraAc)
						contaPontos();
					// contagem de pontos

					// Cria pacote de retorno
					JSONObject jsonObject = new JSONObject();
					jsonObject.put("id", 20);
					jsonObject.put("palavraAceita", palavraAc);
					ArrayList<String> nomes = new ArrayList<>();
					ArrayList<Integer> pontos = new ArrayList<>();
					for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
						nomes.add(kv.getKey());
						pontos.add(kv.getValue().getPontos());
					}
					JSONObject clientes = new JSONObject();
					clientes.put("nomes", nomes);
					clientes.put("pontos", pontos);
					jsonObject.put("clientes", clientes);

					sendAll(jsonObject, null);
					novaRodada();
				}
			}
		}

		private void alteraOrdem() {
			ordem.clear();
			for (int i = 1; i < sorteio.size(); i++) {
				sorteio.get(i).setOrdem(i - 1);
				sorteio.get(i).setErros(6 / sorteio.size()-1);
				sorteio.get(i).setAcertou(false);
				ordem.add(sorteio.get(i));
			}
			sorteio.get(0).setOrdem(sorteio.size() - 1);
			ordem.add(sorteio.get(0));

			sorteio.clear();

			for (Cliente cliente : ordem) {
				sorteio.add(cliente);
			}
		}

		private void novaRodada() throws JSONException {
			boolean key = true;
			String campeao = new String();
			for (Entry<String, Cliente> kv : mapOnlines.entrySet()) {
				if (kv.getValue().getPontos() >= 5) {
					key = false;
					campeao = kv.getKey();
				}
			}
			if (key) {
				alteraOrdem();
				// System.out.println("Mestre: " + ordem.get(0).getNome());
				letrasChutadas.clear();
				adicionaStatus("Mestre: " + ordem.get(0).getNome());
				lblMestre.setText("Mestre: " + ordem.get(0).getNome());
				selecionaMestre(ordem.get(0));
			}else {
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("id", 23);
				jsonObject.put("campeao", campeao);
				sendAll(jsonObject, null);
			}
		}

		private void contaPontos() {
			if (acertaram) {
				for (Cliente cliente : sorteio) {
					if (cliente.isAcertou()) {
						cliente.setPontos(cliente.getPontos() + 1);
					}
				}
			} else {
				ordem.get(0).setPontos(ordem.get(0).getPontos() + 2);
			}

		}

		private void enviaVez() throws JSONException {
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("id", 22);
			try {
				send(jsonObject, ordem.get(vez));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}