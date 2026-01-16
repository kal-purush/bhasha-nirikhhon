package edu.uptc.calculate.control;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JOptionPane;

import edu.uptc.calculate.model.Convert;
import edu.uptc.calculate.view.ActionsViews;
import edu.uptc.calculate.view.MainWindow;

public class ControlCalculate implements ActionListener, ItemListener {
	private Convert model;
	private ActionsViews view;

	public ControlCalculate(Convert model, ActionsViews view) {
		this.model = model;
		this.view = view;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		switch (e.getActionCommand()) {
		case "convert":

			if (view.readDates()[0].toString().equals("")) {
				JOptionPane.showMessageDialog(null, "NO HAY NUMEROS DIGITADOS");
				break;

			}
			if (model.cheackNumbers(view.readDates()[0].toString(), view.readDates()[1].toString())) {
				JOptionPane.showMessageDialog(null,
						"LOS NUMEROS INTRODUCIDOS NO ESTA PERMITIDOS EN LA BASE SELECCIONADA");
				((MainWindow) view).clearNumbers();

				break;

			}

			if (view.readDates()[1].toString().equals(view.readDates()[2].toString())) {
				JOptionPane.showMessageDialog(null, "LAS BASES NUMERICAS SON IGUALES");
				view.writeResult(model.convert(view.readDates()[0].toString(), view.readDates()[1].toString(),
						view.readDates()[2].toString()));
				break;
			}

			view.writeResult(model.convert(view.readDates()[0].toString(), view.readDates()[1].toString(),
					view.readDates()[2].toString()));
			break;

		case "delete":
			String a = view.readDates()[0].toString();
			String result = "";
			for (int i = 0; i < a.length() - 1; i++) {
				result += a.charAt(i);
			}

			((MainWindow) view).clearNumbers();

			((MainWindow) view).writeNumbers(result);
			break;
		case "deleteAll":
			((MainWindow) view).clearNumbers();
			break;
		case "0":
			((MainWindow) view).writeNumbers("0");
			break;
		case "1":
			((MainWindow) view).writeNumbers("1");
			break;
		case "2":
			((MainWindow) view).writeNumbers("2");
			break;
		case "3":
			((MainWindow) view).writeNumbers("3");
			break;
		case "4":
			((MainWindow) view).writeNumbers("4");
			break;
		case "5":
			((MainWindow) view).writeNumbers("5");
			break;
		case "6":
			((MainWindow) view).writeNumbers("6");
			break;
		case "7":
			((MainWindow) view).writeNumbers("7");
			break;
		case "8":
			((MainWindow) view).writeNumbers("8");
			break;
		case "9":
			((MainWindow) view).writeNumbers("9");
			break;
		case "A":
			((MainWindow) view).writeNumbers("A");
			break;
		case "B":
			((MainWindow) view).writeNumbers("B");
			break;
		case "C":
			((MainWindow) view).writeNumbers("C");
			break;
		case "D":
			((MainWindow) view).writeNumbers("D");
			break;
		case "E":
			((MainWindow) view).writeNumbers("E");
			break;
		case "F":
			((MainWindow) view).writeNumbers("F");
			break;

		case "information":
			new edu.uptc.calculate.view.MainMenu().getInformation();
			break;

		case "EXIT":
			new edu.uptc.calculate.view.MainMenu().getInformation();
			break;

		}

	}

	@Override
	public void itemStateChanged(ItemEvent e) {
		switch (e.getItem().toString()) {
		case "SELECCIONAR":
			((MainWindow) view).DisableBtn();
			break;

		case ActionsViews.BASE_BINARY:
			((MainWindow) view).binary();

			break;
		case ActionsViews.BASE_DECIMAL:
			((MainWindow) view).decimal();

			break;
		case ActionsViews.BASE_OCTAL:
			((MainWindow) view).octal();

			break;
		case ActionsViews.BASE_HEXADECIMAL:
			((MainWindow) view).HexaDecimal();

			break;

		}
	}
}