import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;

public class Ball {
	//���λ�ã���������
	private int x,y;
	//dx��dy�൱�ڷ�������
	private int dx = 2,dy = 2;
	//��Ĵ�С
	private final int SIZE = 20;
	
	public Ball(int x , int y ) {
		this.x = x;
		this.y = y;
	}
	
	public void move() {
		x += dx;
		y += dy;
	}
	
	public void reverseX(){
		dx =-dx;
	}
	public void reverseY(){
		dy =-dy;
	}
	//��ͼС��
	public void paint(Graphics g) {
		g.setColor(Color.YELLOW);
		g.fillOval(x, y, SIZE, SIZE);
	}
	//��С������Ӿ��ΰ�װһ�£������ж���ײ
	public Rectangle getBounds() {
		
		return new Rectangle(x,y,SIZE,SIZE);
		
	}
	public int getX() {
		return x;
	}
	public int getY() {
		return y;
	}
}