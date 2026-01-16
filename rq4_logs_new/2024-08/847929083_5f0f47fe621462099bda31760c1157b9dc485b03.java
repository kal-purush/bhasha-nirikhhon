package org.jlz.Controlador;

import org.jlz.GuiCalculadora;
import org.jlz.Model.Calculadora;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class SumarController {
  //1. Crea como atributos un modelo de la suma y la GUI
  private Calculadora modelSumar;
  private GuiCalculadora guiCalculadora;

  // 2. El constructor recibe un modelo y una GUI externas
  // y los asignas a los atributos internos del controlador
  public SumarController(Calculadora modelSumarExterno, GuiCalculadora guiCalculadoraExterno){
    this.modelSumar = modelSumarExterno;
    this.guiCalculadora = guiCalculadoraExterno;
    this.guiCalculadora.setSumOperationEvent(new ListenerButtonSumar());
  }

  //3. Crea el evento para el boton de la interfaz
  class ListenerButtonSumar implements ActionListener{

    @Override
    public void actionPerformed(ActionEvent e) {
      double x, y, resultado = 0.00;
      try {
        x = guiCalculadora.getNumX();
        y = guiCalculadora.getNumY();
        modelSumar.sumarNumeros(x, y);
        guiCalculadora.setResultado(modelSumar.getResultado());

      }
      catch (NumberFormatException ex){
        guiCalculadora.messageError(String.valueOf(ex));
      }
    }
  }
}