
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.ArrayList;
import java.util.Random;
import javax.swing.JPanel;
import javax.swing.Timer;

public class Snake extends JPanel implements ActionListener, KeyListener{
    
    public static final int UP = 0, DOWN = 1, LEFT = 2, RIGHT = 2, delay = 50;
    public Point head, food, badFood;
    public int direction = DOWN;
    public Random ran;
    public Timer timer;
    public ArrayList<Point> snakeBody = new ArrayList<Point>();
    public boolean over = false;
    
    public Snake() {
        super();
        setPreferredSize(new Dimension(800, 800));
        setBackground(Color.BLUE);
        addKeyListener(this);
        initialNormalGameMode();
    }

    @Override
    public void actionPerformed(ActionEvent ae) {
        
    }

    @Override
    public void keyPressed(KeyEvent ke) {
        int keyCode = ke.getKeyCode();
        
        if ((keyCode == KeyEvent.VK_A || keyCode == KeyEvent.VK_LEFT) && direction != RIGHT)
		{
			direction = LEFT;
		}

		if ((keyCode == KeyEvent.VK_D || keyCode == KeyEvent.VK_RIGHT) && direction != LEFT)
		{
			direction = RIGHT;
		}

		if ((keyCode == KeyEvent.VK_W || keyCode == KeyEvent.VK_UP) && direction != DOWN)
		{
			direction = UP;
		}

		if ((keyCode == KeyEvent.VK_S || keyCode == KeyEvent.VK_DOWN) && direction != UP)
		{
			direction = DOWN;
		}
    }
    
    

    private void initialNormalGameMode() {
       over = false;
       direction = DOWN;
       ran = new Random();
       head = new Point(400, 400);
       snakeBody.clear();     
       timer = new Timer(delay, this);
       timer.start();
    }
    
    
    
    @Override
    public void keyTyped(KeyEvent ke) {}

    @Override
    public void keyReleased(KeyEvent ke) {}
}