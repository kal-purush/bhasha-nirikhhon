package game;

import java.awt.BorderLayout;
import java.awt.Frame;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import com.jogamp.opengl.GLCapabilities;
import com.jogamp.opengl.awt.GLCanvas;
import com.jogamp.opengl.util.Animator;

public class GameWindow extends Frame {
	
	public static void main(String[] args) {
		//initialize and display game window
		GameWindow window = new GameWindow();
        window.setVisible(true);
	}
	

	static Animator anim = null;
	private void setupJOGL(){
	    GLCapabilities caps = new GLCapabilities(null);
	    caps.setDoubleBuffered(true);
	    caps.setHardwareAccelerated(true);
	    
	    GLCanvas canvas = new GLCanvas(caps); 
        add(canvas);

//        JoglEventListener jgl = new JoglEventListener();
//        canvas.addGLEventListener(jgl); 
//        canvas.addKeyListener(jgl); 
//        canvas.addMouseListener(jgl);
//        canvas.addMouseMotionListener(jgl);

        anim = new Animator(canvas);
        anim.start();

	}
	
    public GameWindow() {
        super("The White Point Guard Special - A Free Throw Simulator");
        setLayout(new BorderLayout());
        addWindowListener(new WindowAdapter()
        {
            public void windowClosing(WindowEvent e)
            {
                System.exit(0);
            }
        });
        setSize(800, 800);
        setLocation(40, 40);
        setVisible(true);
        setupJOGL();
    }
	
	//auto-generated ID (not used, I just don't like eclipse warnings)
	private static final long serialVersionUID = 1L;

}