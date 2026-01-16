package gui.ex24;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.ListCellRenderer;

public class CellRenderer extends JLabel implements ListCellRenderer{

	CellRenderer(){
      setOpaque(true);
    }

    public Component getListCellRendererComponent(
            JList list,
            Object value,
            int index,
            boolean isSelected,
            boolean cellHasFocus){

      ComboLabel data = (ComboLabel)value;
      setText(data.getText());
      setIcon(data.getIcon());

      if (isSelected){
        setForeground(Color.white);
        setBackground(Color.black);
      }else{
        setForeground(Color.black);
        setBackground(Color.white);
      }

      return this;
    }
  }