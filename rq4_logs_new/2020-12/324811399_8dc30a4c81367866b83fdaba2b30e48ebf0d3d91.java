import java.awt.Color;
import java.awt.Font;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;

import javax.imageio.ImageIO;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;
import javax.sound.sampled.LineUnavailableException;
import javax.sound.sampled.UnsupportedAudioFileException;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.LineBorder;
import javax.swing.border.TitledBorder;


abstract class HW5GameObject{ // ���ӿ� �ʿ��� object��
	abstract void draw(Graphics g);
	void update(float dt) {};
	void collisionHandling(HW5GameObject in) {};
	boolean ball = false;
	boolean isDead() {return false;}
	boolean block = false;
	boolean special_block = false;
	int now_x;
	int now_y;
	boolean outball = false;
	boolean bound = false;
}

// ���ӹ�
class HW5Bar extends HW5GameObject{
	
	int x,y;
	int w,h;
	Color color;
	
	Image image;
	

	HW5Bar(int _x, int _y, int _w, int _h, Color c){
		x = _x;
		y = _y;
		w = _w;
		h = _h;
		color = c;
		
		try {
//			image = ImageIO.read(new File("lilies.jpg"));

			URL imageUrl = getClass().getClassLoader().getResource("bar.png");
			image = ImageIO.read(imageUrl);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	@Override
	void draw(Graphics g) {
		// TODO Auto-generated method stub
		g.setColor(color);
		g.fillRect(x, y, w, h);
		g.drawImage(image,x,y,null);
	}
	boolean isCollided(HW5Ball b) {
		if(x < b.x + b.r && b.x - b.r < x+w && b.y+b.r > y && b.y -b.r < y+h) {
			return true;
		}
		return false;
	}
	
}

//���ϵ�
class HW5Block extends HW5GameObject{
	
	int x,y;
	int w,h;
	Color color;
	Color color2;
	boolean coll = false;
	Image image;
	
	HW5Block(int _x, int _y, int _w, int _h, Color c,Color c2,boolean special){
		x = _x;
		y = _y;
		w = _w;
		h = _h;
		color = c;
		color2 = c2;
		block = true;
		special_block = special;
		
		now_x = _x + _w/2;
		now_y = _y + _h;
		
	}
	@Override
	void draw(Graphics g) {
		// TODO Auto-generated method stub
		Graphics2D g2 = (Graphics2D) g;
		g.setColor(color2);
		g.fillRect(x, y, w, h);
		g.setColor(color);
		g.fillRect(x+3, y+3, w-6, h-6);
	}
	boolean isCollided(HW5Ball b) {
		if(x < b.x + b.r && b.x - b.r < x+w && b.y+b.r > y && b.y -b.r < y+h) {
			return true;
		}
		return false;
	}
	@Override 
	boolean isDead() {
		if(coll == true)
			return true;
		return false;
	}
}

// ���� â ��

class HW5Wall extends HW5GameObject{
	
	int x,y;
	int w,h;
	Color color;

	HW5Wall(int _x, int _y, int _w, int _h, Color c){
		x = _x;
		y = _y;
		w = _w;
		h = _h;
		color = c;
		
	}
	@Override
	void draw(Graphics g) {
		// TODO Auto-generated method stub
		g.setColor(color);
		g.fillRect(x, y, w, h);  
	}
	boolean isCollided(HW5Ball b) {
		if(x < b.x + b.r && b.x - b.r < x+w && b.y+b.r > y && b.y -b.r < y+h) {
			return true;
		}
		return false;
	}
	
}

//��
class HW5Ball extends HW5GameObject{

	float x, y;
	float prev_x, prev_y;
	float vx, vy;				// velocity
	float r;
	Color color;

	HW5Ball(float _x, float _y, float _r){
		x = _x;
		y = _y;
		prev_x = x;
		prev_y = y;
		r = _r;
		color = new Color((int)(Math.random()*256), (int)(Math.random()*256), (int)(Math.random()*256));
		vx = (float)(100);
		vy = (float)(400);
		ball = true;
	}
	HW5Ball(float _x, float _y, float _r,float _vx,float _vy){
		x = _x;
		y = _y;
		prev_x = x;
		prev_y = y;
		r = _r;
		color = new Color((int)(Math.random()*256), (int)(Math.random()*256), (int)(Math.random()*256));
		vx = _vx;
		vy = _vy;
		ball = true;
	}
	@Override 
	void draw(Graphics g) {
		g.setColor(color);
		g.fillOval((int)(x-r), (int)(y-r), (int)(2*r), (int)(2*r));
	}
	@Override
	void update(float dt) {
		prev_x = x;
		prev_y = y;
		x = x + vx*dt;
		y = y + vy*dt;
	}
	@Override
	void collisionHandling(HW5GameObject in) {
		if(in == this) return; 
		if(in instanceof HW5Wall) {
			HW5Wall w = (HW5Wall) in;
			if(w.isCollided(this))
			{
				// �Ʒ��ϰ� �浹
				if(prev_y+r<w.y) {
					ball = false;
					outball = true;
				}
				// �����ϰ� �浹
				if(prev_y-r>w.y+w.h) { vy = -vy; y = w.y +w.h + r; bound = true;}

				// �����ϰ� �浹
				if(prev_x+r<w.x) { vx = -vx; x = w.x - r; bound = true;}
				// �����ʸ��ϰ� �浹
				if(prev_x-r>w.x+w.w) { vx = -vx; x = w.x +w.w + r; bound = true;}
				
			}
		}
		// �ٿ��� ƨ�ܳ�����
		if(in instanceof HW5Bar) {
			HW5Bar b = (HW5Bar) in;
			if(b.isCollided(this))
			{
				// �Ʒ�
				if(prev_y-r>b.y+b.h) { 
					vy = -vy; y = b.y +b.h + r; bound = true;

				}
				//����
				if(prev_y+r<b.y) { 
					vy = -vy; y = b.y - r; bound = true;
				}	
				// �����ϰ� �浹
				if(prev_x+r<b.x) { vx = -vx; x = b.x - r; bound = true;}
				// �����ʸ��ϰ� �浹
				if(prev_x-r>b.x+b.w) { vx = -vx; x = b.x +b.w + r; bound = true;}
			}
		}
		
		//�����̶� �ε�����
		if(in instanceof HW5Block) {
			HW5Block bk = (HW5Block) in;
			if(bk.isCollided(this))
			{
				// �Ʒ��ϰ� �浹
				if(prev_y+r<bk.y) { vy = -vy; y = bk.y - r; bk.coll = true;}	
				// �����ϰ� �浹
				if(prev_y-r>bk.y+bk.h) { vy = -vy; y = bk.y +bk.h + r; bk.coll = true;}
				// �����ϰ� �浹
				if(prev_x+r<bk.x) { vx = -vx; x = bk.x - r; bk.coll = true;}
				// �����ʸ��ϰ� �浹
				if(prev_x-r>bk.x+bk.w) { vx = -vx; x = bk.x +bk.w + r; bk.coll = true;}
			}
		}
	}
}

//���� �г� : �����̽��� ������ ���� ����
class HW5StartPanel extends JPanel{
	
	JLabel intro1;
	JLabel intro2;
	JLabel intro3;
	JLabel intro4;

	
	HW5StartPanel(){
		
		setLayout(null);
		
		intro1 = new JLabel("Java Programming");
		intro1.setBounds(124, 70, 800, 80);			
		intro1.setFont(new Font("��������",Font.BOLD,50));
		add(intro1);
		
		intro2 = new JLabel("Homework#5");
		intro2.setBounds(230, 150, 800, 60);
		intro2.setFont(new Font("��������",Font.BOLD,40));
		add(intro2);
		
		intro3 = new JLabel("BLOCK BREAKER");
		intro3.setBounds(78, 300, 800, 150);
		intro3.setFont(new Font("��������",Font.BOLD,60));
		add(intro3);
		
		intro4 = new JLabel("������ �����Ϸ��� �����̽� �ٸ� Ŭ���ϼ���!");
		intro4.setBounds(120, 500, 800, 60);
		intro4.setFont(new Font("��������",Font.BOLD,20));
		add(intro4);
	}
	@Override
	protected void paintComponent(Graphics g) {
		// TODO Auto-generated method stub
		super.paintComponent(g);
		Graphics2D g2 = (Graphics2D) g;
        Color c = new Color(104, 97, 214);
        GradientPaint gp = new GradientPaint
					(0, 0, c, 0,getHeight(),Color.white);
        g2.setPaint(gp);
        g2.fillRect(0, 0, getWidth(), getHeight());
        setBorder(new TitledBorder(new LineBorder(new Color(123, 123, 179),20)));
	}
}

//���� �г�
class HW5GamePanel extends JPanel implements Runnable{
	
	LinkedList <HW5GameObject> objs = new LinkedList<>();
	
	boolean gameover = false;
	
	float dtime = 0.033f;
	
	int ballnum = 1;
	int cnt = 0;
	
	int gamescore = 0;
	
	boolean new_ball = false;
	
	int new_x, new_y;
	
	int level = 1;
	
	Clip jump_clip;
	Clip break_clip;
	Clip break2_clip;
	
	HW5Bar bar;
	
	JLabel end1;
	
	Image image2;
	Image image3;
	
	HW5GamePanel(){
		Color wallcol = new Color(230,230,200);
		objs.add(new HW5Wall(0,0,680,20,wallcol));
		objs.add(new HW5Wall(0,745,680,20,Color.white));
		objs.add(new HW5Wall(0,0,20,780,wallcol));
		objs.add(new HW5Wall(665,0,20,780,wallcol));
		
		bar = new HW5Bar(280,700,140,20,Color.BLUE);
		objs.add(bar);
		
		try {
			
			jump_clip = AudioSystem.getClip();
			
			URL url4 = getClass().getClassLoader().getResource("jump1.wav");
			AudioInputStream audioStream4 = AudioSystem.getAudioInputStream(url4);
			jump_clip.open(audioStream4);
			
			break_clip = AudioSystem.getClip();
			
			URL url5 = getClass().getClassLoader().getResource("break.wav");
			AudioInputStream audioStream5 = AudioSystem.getAudioInputStream(url5);
			break_clip.open(audioStream5);
			
			break2_clip = AudioSystem.getClip();
			
			URL url7 = getClass().getClassLoader().getResource("break2.wav");
			AudioInputStream audioStream7 = AudioSystem.getAudioInputStream(url7);
			break2_clip.open(audioStream7);


		} catch (LineUnavailableException e) {
			e.printStackTrace();
		} catch (UnsupportedAudioFileException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
//			image = ImageIO.read(new File("lilies.jpg"));

			URL imageUrl = getClass().getClassLoader().getResource("bg1.png");
			image2 = ImageIO.read(imageUrl);
			
			URL imageUrl2 = getClass().getClassLoader().getResource("backimage1.jpg");
			image3 = ImageIO.read(imageUrl2);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		Thread t = new Thread(this);
		t.start();
		
	}
	@Override
	protected void paintComponent(Graphics g) {
		// TODO Auto-generated method stub
		super.paintComponent(g);
		g.drawImage(image3,0,0,null);
		g.drawImage(image2,125,50,null);
		
		for(HW5GameObject o:objs) {
			o.draw(g);
		}
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			
			while(true) {
				
				cnt = 0;
				
				for(HW5GameObject o : objs) {
					o.update(dtime);
				}
				for(HW5GameObject o1 : objs) {
					if( o1 instanceof HW5Ball) {
						for(HW5GameObject o2 : objs) {
//							if(o1 != o2) 
							o1.collisionHandling(o2);
							
						}
					}
				}
				Iterator<HW5GameObject> it = objs.iterator();
				while(it.hasNext()) {
					HW5GameObject o = it.next();
					if(o.outball == true) {
						o.ball = false;
						it.remove();
					}
					if(o.ball == true)
						cnt++;
					if(o.bound == true) {
						jump_clip.setFramePosition(0);
						jump_clip.start();
						o.bound = false;
					}
					if(o.isDead()==true) {
						gamescore += 50;
						if(o.special_block == true) {
							new_ball = true;
							new_x = o.now_x;
							new_y = o.now_y;
							break_clip.setFramePosition(0);
							break_clip.start();
						}
						else {
							break2_clip.setFramePosition(0);
							break2_clip.start();
						}
						System.out.println("���ϱ���");
						it.remove();
					}
				}
				if(new_ball == true && level == 1) {
					objs.add(new HW5Ball(new_x-30,new_y+2,10,-100.0f,400.0f));
					objs.add(new HW5Ball(new_x,new_y+2,10,30f,400.0f));
					objs.add(new HW5Ball(new_x+30,new_y+2,10,100.0f,400.0f));
					new_ball = false;
				}
				else if(new_ball == true && level == 2) {
					objs.add(new HW5Ball(new_x-20,new_y+2,8,-100.0f,550.0f));
					objs.add(new HW5Ball(new_x,new_y+2,8,30.0f,550.0f));
					objs.add(new HW5Ball(new_x+20,new_y+2,8,100.0f,550.0f));
					new_ball = false;
				}
				else if(new_ball == true && level == 3) {
					objs.add(new HW5Ball(new_x-10,new_y+2,6,-100.0f,750.0f));
					objs.add(new HW5Ball(new_x,new_y+2,6,30f,750.0f));
					objs.add(new HW5Ball(new_x+10,new_y+2,6,100.0f,750.0f));
					new_ball = false;
				}
				ballnum = cnt;
				if(gamescore >= 450 && gamescore < 2250) {
					level = 2;
				}
				else if(gamescore >= 2250){
					level = 3;
				}
				if(ballnum == 0) {
					level = 1;
				}
				Thread.sleep((int)(dtime*1000));
				repaint();
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

//���ӿ��� �г�, ���� ������
class HW5EndPanel extends JPanel{
	
	JLabel end1;
	JLabel end2;

	HW5EndPanel(){
		
		setLayout(null);
		
		end1 = new JLabel("GameOver");
		end1.setBounds(195, 300, 800, 150);			
		end1.setFont(new Font("��������",Font.BOLD,60));
		add(end1);
		
		end2 = new JLabel("������ �ٽ� �����Ϸ��� �����̽� �ٸ� Ŭ���ϼ���!");
		end2.setBounds(110, 500, 800, 60);
		end2.setFont(new Font("��������",Font.BOLD,20));
		add(end2);
		
	}
	@Override
	protected void paintComponent(Graphics g) {
		// TODO Auto-generated method stub
		super.paintComponent(g);
		Graphics2D g2 = (Graphics2D) g;
        Color c = new Color(104, 97, 214);
        GradientPaint gp = new GradientPaint
					(0, 0, c, 0,getHeight(),Color.white);
        g2.setPaint(gp);
        g2.fillRect(0, 0, getWidth(), getHeight());
        setBorder(new TitledBorder(new LineBorder(new Color(123, 123, 179),20)));
	}
	
}


public class JavaHW5 extends JFrame implements KeyListener,Runnable{
	
	boolean end = false;
	boolean start = true;
	boolean game = false;
	HW5StartPanel StartPanel;
	HW5GamePanel GamePanel;
	HW5EndPanel EndPanel;
	JLabel score;
	Clip start_clip;
	Clip game_clip;
	Clip end_clip;
	Clip up_clip;
	
	public static void main(String[] args) {
		new JavaHW5();
	}
	JavaHW5(){
		setTitle("���ϰ���");
		setSize(700,800);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		
		StartPanel = new HW5StartPanel();
		GamePanel = new HW5GamePanel();
		EndPanel = new HW5EndPanel();
		score = new JLabel();
		
		
		add(StartPanel);
		
		Thread t = new Thread(this);
		t.start();
		
		addKeyListener(this);
		setFocusable(true);
		requestFocus();
		
		setVisible(true);
		
		try {

			start_clip = AudioSystem.getClip();
			
			URL url1 = getClass().getClassLoader().getResource("main_lobby.wav");
			AudioInputStream audioStream1 = AudioSystem.getAudioInputStream(url1);
			start_clip.open(audioStream1);
			
			game_clip = AudioSystem.getClip();
			
			URL url2 = getClass().getClassLoader().getResource("game.wav");
			AudioInputStream audioStream2 = AudioSystem.getAudioInputStream(url2);
			game_clip.open(audioStream2);
			
			end_clip = AudioSystem.getClip();
			
			URL url3 = getClass().getClassLoader().getResource("custom.wav");
			AudioInputStream audioStream3 = AudioSystem.getAudioInputStream(url3);
			end_clip.open(audioStream3);
			
			up_clip = AudioSystem.getClip();
			
			URL url6 = getClass().getClassLoader().getResource("up.wav");
			AudioInputStream audioStream6 = AudioSystem.getAudioInputStream(url6);
			up_clip.open(audioStream6);
			
		} catch (LineUnavailableException e) {
			e.printStackTrace();
		} catch (UnsupportedAudioFileException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		start_clip.setFramePosition(0);
		start_clip.start();
		
		
	}
	@Override
	public void keyTyped(KeyEvent e) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void keyPressed(KeyEvent e) {
		
		// TODO Auto-generated method stub
		//��ŸƮ -> ����ȭ��
		if(e.getKeyCode() == KeyEvent.VK_SPACE && start == true) {
			
			StartPanel.setVisible(false);
			add(GamePanel);
			GamePanel.setVisible(true);
			start = false;
			game = true;
			GamePanel.bar.x = 280;
			GamePanel.objs.add(new HW5Ball(330,650,10));
			
			for(int i= 0 ;i <3; i++) {
				for(int j = 0; j < 3;j++) {
					//Color bc = new Color((int)(Math.random()*256), (int)(Math.random()*256), (int)(Math.random()*256));
					Color bc = new Color(150, 100, 250 );
					Color bc1 = new Color(150, 250, 100 );
					Color bc3 = new Color(100, 50, 200 );
					Color bc2 = new Color(100, 200, 50 );
					int random = (int)(Math.random()*5);
					if(random == 1 || random == 2) {
						GamePanel.objs.add(new HW5Block(20+i*215,21+j*145,214,144,bc1,bc2,true));
					}
					else {
						GamePanel.objs.add(new HW5Block(20+i*215,21+j*145,214,144,bc,bc3,false));
					}
				}
			}
			GamePanel.gamescore = 0;
			start_clip.stop();
			game_clip.setFramePosition(0);
			game_clip.start();
			
		}
		//����ȭ�� �� ������
		if(e.getKeyCode() == KeyEvent.VK_LEFT && game == true) {
			if(GamePanel.bar.x < 55) {
				GamePanel.bar.x = 20;
			}
			else { GamePanel.bar.x = GamePanel.bar.x - 37;}
			repaint();
		}
		if(e.getKeyCode() == KeyEvent.VK_RIGHT && game == true) {
			if(GamePanel.bar.x > 500) {
				GamePanel.bar.x = 525;
			}
			else { GamePanel.bar.x = GamePanel.bar.x + 37;}
			repaint();
		}
		//����ȭ�� -> ����ȭ��
		if(e.getKeyCode() == KeyEvent.VK_SPACE && end == true) {
			EndPanel.setVisible(false);
			StartPanel.setVisible(true);
			end = false;
			start = true;
			end_clip.stop();
			start_clip.setFramePosition(0);
			start_clip.start();
		}
	}
	@Override
	public void keyReleased(KeyEvent e) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			while(true) {
				if(start == true)
					System.out.println("START");
				if(end == true)
					System.out.println("END");
				if(game == true)
					System.out.println("GAME");
				System.out.println(GamePanel.ballnum);
				
				//���ӿ���
				if(game == true && GamePanel.ballnum ==0) {
					Iterator<HW5GameObject> it = GamePanel.objs.iterator();
					while(it.hasNext()) {
						HW5GameObject o = it.next();
						if(o.block == true)
							it.remove();
					}
					score.setText("Score: " + GamePanel.gamescore);
					score.setBounds(130, 100, 800, 150);			
					score.setFont(new Font("��������",Font.BOLD,80));
					EndPanel.add(score);
					
					GamePanel.setVisible(false);
					add(EndPanel);
					EndPanel.setVisible(true);
					game = false;
					start = false;
					end = true;
					game_clip.stop();
					end_clip.setFramePosition(0);
					end_clip.start();
					Thread.sleep((int)(400));
				}
				
				//����2
				else if(game == true && GamePanel.level == 2 && GamePanel.gamescore == 450) {
					up_clip.setFramePosition(0);
					up_clip.start();
					Iterator<HW5GameObject> it = GamePanel.objs.iterator();
					while(it.hasNext()) {
						HW5GameObject o = it.next();
						if(o.block == true || o.ball == true)
							it.remove();
					}
					GamePanel.bar.x = 280;
					GamePanel.objs.add(new HW5Ball(330,650,8,70.0f,550.0f));
					
					for(int i= 0 ;i <6; i++) {
						for(int j = 0; j < 6;j++) {
							//Color bc = new Color((int)(Math.random()*256), (int)(Math.random()*256), (int)(Math.random()*256));
							Color bc = new Color(150, 100, 250 );
							Color bc1 = new Color(150, 250, 100 );
							Color bc3 = new Color(100, 50, 200 );
							Color bc2 = new Color(100, 200, 50 );
							int random = (int)(Math.random()*4);
							if(random == 1) {
								GamePanel.objs.add(new HW5Block(21+i*107,21+j*60,106,59,bc1,bc2,true));
							}
							else {
								GamePanel.objs.add(new HW5Block(21+i*107,21+j*60,106,59,bc,bc3,false));
							}
						}
					}
					Thread.sleep((int)(400));
				}
				
				//����3
				else if(game == true && GamePanel.level == 3 && GamePanel.gamescore == 2250) {
					
					up_clip.setFramePosition(0);
					up_clip.start();
					
					Iterator<HW5GameObject> it = GamePanel.objs.iterator();
					while(it.hasNext()) {
						HW5GameObject o = it.next();
						if(o.block == true || o.ball == true)
							it.remove();
					}
					GamePanel.bar.x = 280;
					GamePanel.objs.add(new HW5Ball(330,650,6,70.0f,750.0f));
					
					for(int i= 0 ;i <10; i++) {
						for(int j = 0; j < 10;j++) {
							//Color bc = new Color((int)(Math.random()*256), (int)(Math.random()*256), (int)(Math.random()*256));
							Color bc = new Color(150, 100, 250 );
							Color bc1 = new Color(150, 250, 100 );
							Color bc3 = new Color(100, 50, 200 );
							Color bc2 = new Color(100, 200, 50 );
							int random = (int)(Math.random()*4);
							if(random == 1) {
								GamePanel.objs.add(new HW5Block(21+i*64,21+j*45,63,44,bc1,bc2,true));
							}
							else {
								GamePanel.objs.add(new HW5Block(21+i*64,21+j*45,63,44,bc,bc3,false));
							}
						}
					}
					Thread.sleep((int)(400));
				}
				
				//���� �Ϸ�
				else if(game == true && GamePanel.gamescore == 7250) {
					Iterator<HW5GameObject> it = GamePanel.objs.iterator();
					while(it.hasNext()) {
						HW5GameObject o = it.next();
						if(o.block == true)
							it.remove();
					}
					score.setText("���ĿϷ�!!!!");
					score.setBounds(130, 100, 800, 150);			
					score.setFont(new Font("��������",Font.BOLD,80));
					EndPanel.add(score);
					
					GamePanel.setVisible(false);
					add(EndPanel);
					EndPanel.setVisible(true);
					game = false;
					start = false;
					end = true;
					game_clip.stop();
					end_clip.setFramePosition(0);
					end_clip.start();
				}
				
				Thread.sleep((int)(800));
				repaint();
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}