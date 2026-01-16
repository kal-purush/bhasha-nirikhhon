package Dev_J_120;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.BevelBorder;

public class Components extends JPanel{

    JTextField textField;
    JButton[] buttons;
    JButton equalButton;
    JButton clearButton;
    boolean start;
    boolean error = false;
    String lastCommand;
    String lastActionCommand;
    BigDecimal result = BigDecimal.ZERO;
    BigDecimal current = BigDecimal.ZERO;
    int counter;
    MathContext mathContext = new MathContext(7);
    
    public Component display(){
        JPanel textPanel = new JPanel(new GridLayout(0, 1, 5, 5));
        textPanel.setBorder(BorderFactory.createEmptyBorder(5, 10, 5, 10));
        textPanel.setOpaque(false); 
        textField = new JTextField();
        textField.setEditable(false);
        textField.setPreferredSize(new Dimension(600, 70));
        textField.setHorizontalAlignment(JTextField.RIGHT);
        textField.setFont(new Font("Arial", Font.BOLD, 55));
        textField.setBorder(BorderFactory.createCompoundBorder(
              BorderFactory.createBevelBorder(BevelBorder.LOWERED),
              BorderFactory.createEmptyBorder(10, 10, 10, 10)));
        textPanel.add(textField);              
     return textPanel;        
    } 
    public Component buttons(){
        String[] buttonText = {"%","7","8","9","\u00F7","\u221A","4","5","6",
                      "\u00D7","\u2192","1","2","3","-","+/-","0","00",".","+"};
        JPanel battonsPanel = new JPanel(new GridLayout(4, 5, 5, 5));
        battonsPanel.setBorder(BorderFactory.createEmptyBorder(5, 10, 0, 10));
        battonsPanel.setOpaque(false);
        buttons = new JButton[buttonText.length];
        for(int i=0; i<buttonText.length; i++) {
            buttons[i] = new JButton(buttonText[i]);
            buttons[i].setPreferredSize(new Dimension(94, 90));
            bordersSet(buttons[i]); 
            buttons[i].setFont(new Font("Arial", Font.BOLD, 50));  
            battonsPanel.add(buttons[i]);
        }
        buttons[0].addActionListener(new ActionCommand(this));
        buttons[1].addActionListener(new ActionInput(this));
        buttons[2].addActionListener(new ActionInput(this));
        buttons[3].addActionListener(new ActionInput(this));
        buttons[4].addActionListener(new ActionCommand(this));
        buttons[5].addActionListener(new ActionCommand(this));
        buttons[6].addActionListener(new ActionInput(this));
        buttons[7].addActionListener(new ActionInput(this));
        buttons[8].addActionListener(new ActionInput(this));
        buttons[9].addActionListener(new ActionCommand(this));
        buttons[10].addActionListener(new ActionInput(this));
        buttons[11].addActionListener(new ActionInput(this));
        buttons[12].addActionListener(new ActionInput(this));
        buttons[13].addActionListener(new ActionInput(this));
        buttons[14].addActionListener(new ActionCommand(this));
        buttons[15].addActionListener(new ActionInput(this));
        buttons[16].addActionListener(new ActionInput(this));
        buttons[17].addActionListener(new ActionInput(this));
        buttons[18].addActionListener(new ActionInput(this));
        buttons[19].addActionListener(new ActionCommand(this));
        
      return battonsPanel;
    }
    public Component equalButton(){
        JPanel equalPanel = new JPanel();
        equalPanel.setLayout(new BoxLayout(equalPanel, BoxLayout.X_AXIS)); 
        equalPanel.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 10));
        equalPanel.setOpaque(false);
        equalButton = new JButton("=");
        equalButton.setPreferredSize(new Dimension(400, 90));
        bordersSet(equalButton); 
        equalButton.setFont(new Font("Arial", Font.BOLD, 50));
        equalButton.addActionListener(new ActionCommand(this)); 
        clearButton = new JButton("C");
        clearButton.setForeground(Color.red); 
        clearButton.setPreferredSize(new Dimension(94, 90));
        bordersSet(clearButton); 
        clearButton.setFont(new Font("Arial", Font.BOLD, 50));
        clearButton.addActionListener(new ActionInput(this)); 
        equalPanel.add(clearButton);
        equalPanel.add(Box.createRigidArea(new Dimension(5,0)));
        equalPanel.add(equalButton);
      return equalPanel;  
    }
    public static void bordersSet(JButton button){
        button.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createBevelBorder(BevelBorder.RAISED),
                BorderFactory.createEmptyBorder(5, 10, 5, 10)));   
    }
    
    
        
public class ActionInput implements ActionListener{
        Components components;

        public ActionInput(Components components) {
            this.components = components;
            textField.setText("0");
            lastCommand = "input";
        }
        @Override
        public void actionPerformed(ActionEvent e) {
            
        if(lastCommand.equals("command") && !e.getActionCommand().equals("+/-"))
           textField.setText("0"); 

        start = textField.getText().equals("0"); 
        String b;
        Character a = e.getActionCommand().charAt(0);
        if(Character.isDigit(a) && e.getActionCommand().length() == 1) {
            if(start) {    
               textField.setText("");
               textField.setText(a.toString()); }
            else {
               b = textField.getText();               
               b += a.toString();
               textField.setText(b); }              
           }
        if(e.getActionCommand().equals("00"))
            doubleZero();
        if(e.getActionCommand().equals("+/-"))
            inverse();
                    
        switch (a) {
            case 'C': 
                textField.setText("0");
                lastCommand = "input";
                result = BigDecimal.ZERO;
                current = BigDecimal.ZERO;
                counter = 0;
                break;
            case '\u2192':
                delete();  
                break; 
            case '.':
                dot();
                break;
                       }
        lastCommand = "input";
        current = new BigDecimal(textField.getText());
        System.out.println("i lastcommand " + lastCommand);
        System.out.println("i lastActionCommand = " + lastActionCommand);
        System.out.println("i current = " + current.toString());
        System.out.println("i result = " + result.toString());
    }
    public void dot(){
        String s;
        if(start)
            textField.setText("0.");
        else
            if(!textField.getText().contains(".")) {
               s = textField.getText();
               textField.setText(s + ".");    }
    }
     public void delete(){
        String s = textField.getText();
        if(s.length()>1) {
           s = s.substring(0, s.length()-1);
           textField.setText(s);}
        else
           textField.setText("0"); 
    }
     public void doubleZero(){
        String s;
        if(!start) {
           s = textField.getText();
           textField.setText(s + "00"); } 
    }
     public void inverse(){
         BigDecimal temp = new BigDecimal(textField.getText());
         temp = temp.multiply(new BigDecimal(-1));
         textField.setText(temp.toString());
      }
 }
        
public class ActionCommand implements ActionListener {
        Components components;

        public ActionCommand(Components components) {
            this.components = components;
        }        
        @Override
        public void actionPerformed(ActionEvent e) {
            String currAction = e.getActionCommand();
            if((currAction.equals("+") || currAction.equals("-") || currAction.equals("\u00D7") || 
               currAction.equals("\u00F7")) && !lastCommand.equals("command")) 
            { 
                if(counter > 0) {
                   current = new BigDecimal(textField.getText());  
                   switch(lastActionCommand) {
                          case "+":
                          result = result.add(current, mathContext);
                          break;
                          case "-":
                          result = result.subtract(current, mathContext);
                          break;
                          case "\u00D7":
                          result = result.multiply(current, mathContext);
                          break;
                          case "\u00F7":
                              try 
                                  {result = result.divide(current, mathContext);}
                              catch(ArithmeticException ae) 
                                   {error = true;}
                              break; } 
                   mathematicalError();
                   lastActionCommand = e.getActionCommand();
                }
                else 
                    result = new BigDecimal(textField.getText(), mathContext);
                counter++;
                lastCommand = "command";
                lastActionCommand = e.getActionCommand();
            }
            else if (currAction.equals("\u221A")) {
                 try 
                    {current = new BigDecimal(textField.getText(), mathContext);  
                     double currentSqrt = Math.sqrt(current.doubleValue());
                     result = new BigDecimal(currentSqrt, mathContext);  }
                 catch (NumberFormatException ne)
                       {error = true;}
                 lastCommand = "input";
                 mathematicalError();
                 lastActionCommand = e.getActionCommand();
            }
            else if(currAction.equals("=") && !lastCommand.equals("command")) {
                switch(lastActionCommand) {
                          case "+":
                          result = result.add(current, mathContext);
                          break;
                          case "-":
                          result = result.subtract(current, mathContext);
                          break;
                          case "\u00D7":
                          result = result.multiply(current, mathContext);
                          break;
                          case "\u221A":
                          break;
                          case "\u00F7":
                              try 
                                  {result = result.divide(current, mathContext);}
                              catch(ArithmeticException ae) 
                                   {error = true;}
                              break; } 
                   mathematicalError();
                   result = BigDecimal.ZERO;
                   current = BigDecimal.ZERO;
                   lastActionCommand = e.getActionCommand();
                   lastCommand = "command";
                   counter = 0;
                   
            }
            System.out.println(counter);
            System.out.println("lastActioncommand " + lastActionCommand);
            System.out.println("result = " + result.toString());
            System.out.println("current = " + current.toString());
            System.out.println("lastcommand " + lastCommand);
        }

        public void percent(){
            
        }

        public  void mathematicalError(){
            if(!error)
               if(result.compareTo(BigDecimal.ZERO) == 0) 
                  textField.setText("0");
               else if(result.compareTo(BigDecimal.ONE.multiply(new BigDecimal(-1))) == 1 && result.compareTo(BigDecimal.ONE) == -1)
                   textField.setText(result.setScale(10, RoundingMode.CEILING).toString());
               else
                  textField.setText(result.toString());   
            else  {
                textField.setText("error");
                error = false;
                lastCommand = "command";
                result = BigDecimal.ZERO;
                current = BigDecimal.ZERO;
                counter = -1;  }       
        }
 }
}