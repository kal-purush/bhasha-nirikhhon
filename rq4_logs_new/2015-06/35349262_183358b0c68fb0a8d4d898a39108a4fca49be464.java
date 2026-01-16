package Main;
import static org.lwjgl.opengl.GL11.*;

import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.util.Random;

import org.lwjgl.LWJGLException;
import org.lwjgl.input.Keyboard;
import org.lwjgl.input.Mouse;
import org.lwjgl.opengl.Display;
import org.lwjgl.opengl.DisplayMode;
import org.lwjgl.opengl.GL11;


public class ColorSimulation {

//	private final int WIDTH = (int)Toolkit.getDefaultToolkit().getScreenSize().getWidth();
//	private final int HEIGHT = (int)Toolkit.getDefaultToolkit().getScreenSize().getHeight();
	private final int WIDTH = 1024;//SHOULD BE EVEN
	private final int HEIGHT = WIDTH/2;
	
	private final int X = 128;//SHOULD BE AN EVEN DIVISOR OF THE WIDTH
	private final int Y = X/2;
	
	private final int length = HEIGHT/Y;
	
	private int time;
	private float interval = .5f;
	private boolean display = false;
	
	private float box[][] = new float[X][Y];
	private float change[][] = new float[X][Y];
	private int ran = 600;//The higher ran is, the lower the randomness
	
	float f0 = .4f;
	private Random rn = new Random();
	
	public void render(){
		if (Keyboard.isKeyDown(Keyboard.KEY_SPACE)){
			display = true;
		} else {
			display = false;
		}
//		update();
		if ((int) System.currentTimeMillis() - time >= interval*5000){
			check();
			time = (int) System.currentTimeMillis();
		}
		
		display();
		f0 = displayScroll(f0,WIDTH/2,HEIGHT/3,display);
		interval = displayScroll(interval,WIDTH/2,2 * HEIGHT/3,display);
		
	}//render method
	
	public void update(){
		for (int a=0;a<X;a++){
			for (int b=0;b<Y;b++){
				box[a][b] = change[a][b];
				change[a][b] = box[a][b];
				if (box[a][b]<0){
					box[a][b] = 0;
				} else if (box[a][b]>1){
					box[a][b] = 1;
				}
			}
		}
	}//update method
	public void check(){
		for (int a=0;a<X;a++){
			for (int b=0;b<Y;b++){
				float c = box[a][b];
				float left,right,up,down;
				
				if (a==0){//If the box is on the far left
					left = box[X-1][b];
				} else {
					left = box[a-1][b];
				}
				if (a==X-1){
					right = box[0][b];
				} else {
					right = box[a+1][b];
				}
				if (b==0){
					up = box[a][Y-1];
				} else {
					up = box[a][b-1];
				}
				if (b==Y-1){
					down = box[a][0];
				} else {
					down = box[a][b+1];
				}
				
//				float avg = (left+right+up+down)/4;
				float nb = left+right+up+down;
				
				float f = 1/ (f0 * f0);
				for (int i=0;i<nb;i++){
					f *= f0;
				}
				float r = rn.nextFloat();
//				System.out.println(r);
				if (f < (1+f) * r){
					box[a][b] = 1;
				} else {
					box[a][b] = 0;
				}
				
//				change[a][b] = (rn.nextInt(ran)==0 ? rn.nextFloat() :(avg-c)/10);
			}
		}
		
//		f0 *= .95f;
	}//check method
	public void display(){
		for (int a=0;a<X;a++){
			for (int b=0;b<Y;b++){
				
				float c = box[a][b];
				if (c<=.5f) glColor3f(1,0,0);
				if (c>=.5f) glColor3f(0,0,1);
				
				int x = a*(WIDTH/X);
				int y = b*(HEIGHT/Y);
				
				glBegin(GL_QUADS);
					glVertex2i(x,y);
					glVertex2i(x+length,y);
					glVertex2i(x+length,y+length);
					glVertex2i(x,y+length);
				glEnd();
			}
		}
		
		
	}//display method
	
	public float displayScroll(float value, int x,int y,boolean display){
		int w = 40;
		int h = 80;
		int bar = h*2/3;
		int W = w*2/3;
		int p = (int) ((bar*2)-(value*bar*2));
		if (display){
			glPushMatrix();
				glTranslatef(x,y,0);
			
				glColor3f(0.4f,0.4f,0.4f);
				glBegin(GL_QUADS);
					glVertex2i(-w,-h);
					glVertex2i(w,-h);
					glVertex2i(w,h);
					glVertex2i(-w,h);
				glEnd();
				glColor3f(1,1,1);
				glBegin(GL_QUADS);
					glVertex2i(-4,-bar);
					glVertex2i(4,-bar);
					glVertex2i(4,bar);
					glVertex2i(-4,bar);
				glEnd();
				glColor3f(0,0,0);
				glBegin(GL_QUADS);
					glVertex2i(-W,-bar+p-10);
					glVertex2i(W,-bar+p-10);
					glVertex2i(W,-bar+p+10);
					glVertex2i(-W,-bar+p+10);
				glEnd();
			glPopMatrix();
		}
//		while(Mouse.next()){
			if (Mouse.isButtonDown(0)){
//				System.out.printf("f0 = %s, x: %s, X: %s, y: %s, Y: %s\n",f0,x-W,Mouse.getX(),(y-bar)+(p-10),HEIGHT-Mouse.getY());
				if (x-W <= Mouse.getX() && 
					Mouse.getX() <= x+W 
					&& (y-bar)+(p-10) <= HEIGHT - Mouse.getY() &&
					HEIGHT - Mouse.getY() <= (y-bar)+(p+10)
					){

					float s1 = ((HEIGHT-Mouse.getY()-y-bar)-(bar*2));
					float s2 = -bar*2;
					value = s1/s2;
					value -= 1;
//					System.out.println(value);
					if (value < 0.001f ){
						value = 0.001f;
					} else if (1 < value){
						value = 1;
					}
			
				}
//			}
			
		}
		return value;
	}//displayScroll method
	public void load(){
		for (int a=0;a<X;a++){
			for (int b=0;b<Y;b++){
				if ((a+b & 1)==0){
					box[a][b] = 0f;
//					System.out.println("BLACK "+((a+b)/2)+" : "+a+" : "+b);
				} else {
					box[a][b] = 1f;
//					System.out.println("WHITE"+a+" : "+b);
				}
			}
		}
		
	}//load method
	
	public void start(){
		initGL();
		load();
		time = (int) System.currentTimeMillis();
		
		while(!Display.isCloseRequested() && !Keyboard.isKeyDown(Keyboard.KEY_ESCAPE)){
			glClear(GL_COLOR_BUFFER_BIT);

			render();
			
			
			Display.update();
			Display.sync(30);
		}
		Display.destroy();
		System.exit(0);
		
	}//start method
	public void initGL(){
		try {
			Display.setDisplayMode(new DisplayMode(WIDTH,HEIGHT));
			Display.setTitle("LWJGL APPLICATION ["+WIDTH+", "+HEIGHT+"]");
			Display.setInitialBackground(0, 255, 100);
			Display.create();
		} catch (LWJGLException e){
			e.printStackTrace();
		}
			GL11.glEnable(GL_BLEND);
			GL11.glBlendFunc(GL11.GL_SRC_ALPHA, GL11.GL_ONE_MINUS_SRC_ALPHA);
			GL11.glDisable(GL_BLEND);
			
			glMatrixMode(GL_PROJECTION);
			glLoadIdentity();
			glOrtho(0,WIDTH,HEIGHT,0,1,-1);
			glMatrixMode(GL_MODELVIEW);
	}//initGL method
	public static void main(String Args[]){
		ColorSimulation app = new ColorSimulation();
		app.start();
	}//main method
	
}//ColorSimulation class