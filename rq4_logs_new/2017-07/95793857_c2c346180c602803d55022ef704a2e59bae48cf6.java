/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package component_editor;

import java.awt.Component;
import javax.swing.DefaultCellEditor;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;

/**
 *
 * @author francisunoxx
 */
public class JSpinnerEditor_Curriculum extends DefaultCellEditor
{
    private JSpinner spinner;
    Double current = new Double(1.00);
    Double min = new Double(0.00);
    Double max = new Double(10.00);
    Double step = new Double(0.25);
    
    public JSpinnerEditor_Curriculum() 
    {
        super(new JTextField());
        spinner = new JSpinner();
        spinner.setModel(new SpinnerNumberModel(current, min, max, step));
    }
    
    // Prepares the spinner component and returns it.
    @Override
    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row,int column)
    {
        return spinner;
    }
    
    //Returns the value contained in the editor.
    @Override
    public Object getCellEditorValue() 
    {
        return spinner.getValue();
    }
    
}