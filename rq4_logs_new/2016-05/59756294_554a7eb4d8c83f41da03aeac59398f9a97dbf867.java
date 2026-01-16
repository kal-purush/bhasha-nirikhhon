import java.awt.*;
import java.awt.image.*;
import java.text.*;
import java.util.*;
import javax.swing.*;
import java.io.*;
import javax.imageio.*;
 
/** Purpose: The purpose of this class is to create the panels
  * used in the levels of the game. This class creates the buttons,
  * the panels, and it adds the background to the panel.
  * 
  *  @author Top Of The Stack(C Liu)
  *  @version 1 05.20.16 Spent 4 hours(Fixing images, testing , etc.)
  */ 
//implements ActionListener
public class BasketFun extends JPanel {
  
  private int levelNum;
  private String backName;
  private Color bCol;
  /**Purpose: The purpose of this method is to construct the
    * JFrame, set the size, and add this panel to the JFrame
    * of Level 1.
    * It also creates all the buttons, adds the Flow Layout 
    * to the panel, and allows the panel to be visible.
   */
  public void makePanel1()
  {
 
    add(makeButtons("Pause","Click here to pause the game!"));
     add(makeButtons("Apple","Click here to drop one apple into the basket!"));
     add(makeButtons("Orange","Click here to drop one orange into the basket!"));
    add(makeButtons("Banana","Click here to drop one banana into the basket!"));
     add(makeButtons("Grape","Click here to drop one grape into the basket!"));
     add(makeButtons("Watermelon","Click here to drop one watermelon into the basket!"));
    
    revalidate();
    repaint();
  }
  
/**Purpose: The purpose of this method is to construct the
     BasketFun class.
   */
 
   public BasketFun(int levelNum, String bName, Color s) { 
     super();
     this.levelNum=levelNum;
     backName=bName;
     bCol=s;
     System.out.println(backName);
    JFrame j=new JFrame("A Basket Full Of Fun: Level 1");
    j.setSize(1000,850);
    this.setPreferredSize(new Dimension( 1000,850));
    j.add(this);
    j.setVisible (true);
    
    FlowLayout f=new FlowLayout();
    f.setHgap(40);
    f.setAlignment (FlowLayout.LEFT);
    this.setLayout(f);
    if (levelNum==1)
    {
    makePanel1();
    }
    else if (levelNum==2)
    {
      makePanel2();
      
    }
    else{
      
      
    }
  }  
   
  public void paintComponent( Graphics g)
  {
     
    BufferedImage b=null;
    super.paintComponent(g);
    try{
      b= ImageIO.read(new File (backName+".jpg"));
      g.drawImage(b,0,0,null);
    }
    catch(IOException e){
      System.out.println("HI");
    }
   
  }
   
 
   /**Purpose: The purpose of this method is to construct the
    * JFrame, set the size, and add this panel to the JFrame
    * of Level 2.
    * It also creates all the buttons, adds the Flow Layout 
    * to the panel, and allows the panel to be visible.
   */
  public void makePanel2()
  {
    add(makeButtons("Pause","Click here to pause the game!"));
     add(makeButtons("Red","Click here to drop one apple into the basket!"));
     add(makeButtons("Yellow","Click here to drop one orange into the basket!"));
     add(makeButtons("Green","Click here to drop one banana into the basket!"));
     add(makeButtons("Octopus","Click here to drop one grape into the basket!"));
     add(makeButtons("Crab","Click here to drop one watermelon into the basket!"));
     revalidate();
     repaint();
  }
  
  
   
  /**Purpose: The purpose of this method is to create the 
    * buttons in the panels. This method adds images to the buttons
    * in the panels.
    * 
    * @param imageName String passes in the image name
    * @param text String passes in text for the tool tip
   */
  public JButton makeButtons(String imageName,String text)
  {
    ImageIcon icon=new ImageIcon(this.getClass().getResource( imageName+".jpg"));
    JButton button=new JButton(icon);
    button.setBackground(bCol);
    button.setToolTipText(text);
    return button;
  }
  
//  public void actionPerformed(ActionEvent e)
//  {
//    
//    
//  }
  
  //forest green;new Color(37,177,77)
  //ocean 0,126,255
  
  public static void main(String[] args) { 
    BasketFun s=new BasketFun (2,"back2", new Color (0,126,255));
  }
  
  
  
  
}