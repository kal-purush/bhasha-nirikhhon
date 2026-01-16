package view;
import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.SQLException;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import model.PedidoDAO;

import javax.swing.JButton;
import javax.swing.JTextField;
import javax.swing.JTable;
import javax.swing.JScrollPane;
import javax.swing.JRadioButton;
import javax.swing.ButtonGroup;

public class JanelaConsultaPedido extends JFrame implements ActionListener {

	private JPanel contentPane;
	private JTextField txtBuscarPedido;
	private JTable table;
	private JScrollPane scrollPane;
	private JButton btnDeletar;
	private JRadioButton rdbtnTema;
	private JRadioButton rdbtnCliente;
	private final ButtonGroup buttonGroup = new ButtonGroup();


	/**
	 * Create the frame.
	 * @throws SQLException 
	 */
	public JanelaConsultaPedido() throws SQLException {
		setBounds(100, 100, 800, 474);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		JPanel panel = new JPanel();
		contentPane.add(panel, BorderLayout.SOUTH);
		
		rdbtnTema = new JRadioButton("Tema");
		rdbtnTema.setSelected(true);
		buttonGroup.add(rdbtnTema);
		panel.add(rdbtnTema);
		
		rdbtnCliente = new JRadioButton("Cliente");
		buttonGroup.add(rdbtnCliente);
		panel.add(rdbtnCliente);
		
		JButton btnBuscarPedido = new JButton("Buscar");
		panel.add(btnBuscarPedido);
		
		txtBuscarPedido = new JTextField();
		panel.add(txtBuscarPedido);
		txtBuscarPedido.setColumns(20);
		
		btnDeletar = new JButton("Deletar");
		btnDeletar.addActionListener(this);
		panel.add(btnDeletar);
		
		PedidoDAO dao = new PedidoDAO();
		
		table = new JTable(dao.listar(null));
		scrollPane = new JScrollPane(table);
		scrollPane.setToolTipText("");
		contentPane.add(scrollPane, BorderLayout.CENTER);
				
		setVisible(true);
	}

	@Override
	public void actionPerformed(ActionEvent ev) {

		if (ev.getSource() == btnDeletar) {
			//JOptionPane.showMessageDialog(null, table.getValueAt(table.getSelectedRow(), 0));
			try {
				PedidoDAO dao = new PedidoDAO();
				int idPedido = Integer.parseInt((String)table.getValueAt(table.getSelectedRow(), 0));
				dao.deletar(idPedido);
				JOptionPane.showMessageDialog(null, "Pedido Deletado Com Sucesso!");
				this.dispose();
				new JanelaConsultaPedido();
			} catch (SQLException e) {
				JOptionPane.showMessageDialog(null, "Erro ao Deletar");
				e.printStackTrace();
			}
		}
	}

}