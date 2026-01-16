import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by prolab on 12/13/16.
 */
public class GUI extends JFrame implements MouseListener {

    ArrayList<Color> colors = new ArrayList<Color>();

    public GUI(){
        super();
        this.setColors();
        Rectangle window = this.getWindowSize();
        this.setBounds(window.width-70,window.height-50,50,50);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        this.setUndecorated(true);
        this.setBackground(colors.get(0));

        this.setAlwaysOnTop(true);
        this.setVisible(true);

        addMouseListener(this);
    }

    private void setColors(){
        colors.add(new Color(127,127,255,100));
        colors.add(new Color(127,255,127,100));
        colors.add(new Color(255,100,20,150));
        colors.add(new Color(127,127,255,170));
    }

    private Rectangle getWindowSize(){
        GraphicsEnvironment env = GraphicsEnvironment.getLocalGraphicsEnvironment();
        Rectangle rect = env.getMaximumWindowBounds();
        return rect;
    }

    public void mouseClicked(MouseEvent e){
        int state = e.getButton();
        ExecutorService service = null;
        if(state == MouseEvent.BUTTON1) {
            try{
                service = Executors.newSingleThreadExecutor();
                service.execute(new SendingPass(new SendingPass.Callback() {
                    @Override
                    public void setOK() {
                        setBackground(colors.get(1));
                    }

                    @Override
                    public void setNG(){
                        setBackground(colors.get(2));
                    }

                    @Override
                    public void reset(){
                        setBackground(colors.get(0));
                    }
                }));
            } finally {
                service.shutdown();
            }
        }
    }

    public void mouseEntered(MouseEvent e){
        this.setBackground(colors.get(3));
    }

    public void mouseExited(MouseEvent e){
        this.setBackground(colors.get(0));
    }

    public void mousePressed(MouseEvent e){}
    public void mouseReleased(MouseEvent e){}

}