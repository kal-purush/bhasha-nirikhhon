import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class Calculadora extends JFrame implements ActionListener {
    private JTextField pantalla;
    private JPanel panelBotones;
    private double resultado;
    private String operacion;
    private boolean nuevaOperacion = true;

    public Calculadora(){
        setTitle("Calculadora");
        setSize(300, 400);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        crearInterfaz();
    }

    private void crearInterfaz(){
        pantalla = new JTextField("0", 20);
        pantalla.setEditable(false);

        pantalla.setHorizontalAlignment(JTextField.RIGHT);
        add(pantalla, BorderLayout.NORTH);

        panelBotones = new JPanel();
        panelBotones.setLayout(new GridLayout(5,4,5,5));

        String[] Botones = {
            "7", "8", "9", "/",
            "4", "5", "6", "*",
            "1", "2", "3", "-",
            "0", ".", "=", "+",
            "C"
        };

        for (String boton : Botones){
            addButton(boton);
        }

        add(panelBotones, BorderLayout.CENTER);
    }

    private void addButton(String label){
        JButton boton = new JButton(label);
        boton.addActionListener(this);
        panelBotones.add(boton);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        String comando = e.getActionCommand();

        if ("0123456789.".contains(comando)){
            if (nuevaOperacion){
                pantalla.setText(comando);
                nuevaOperacion = false;
            }

            else {
                pantalla.setText(pantalla.getText() + comando);
            }
        } else if ("+-*/".contains(comando)){
            if (!nuevaOperacion){
                calcularOperacion();
                operacion = comando;
                resultado = Double.parseDouble(pantalla.getText());
                nuevaOperacion = true;
            }
        } else if (comando.equals("=")){
            calcularOperacion();
            operacion = "";
        } else if (comando.equals("C")){
            resultado = 0;
            pantalla.setText("0");
            nuevaOperacion = true;
        }

    }
}