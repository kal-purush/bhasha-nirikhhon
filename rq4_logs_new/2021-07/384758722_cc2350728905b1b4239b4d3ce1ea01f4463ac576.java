import java.awt.Point;

public class Circle extends Shape{
	private int rad;
	public Circle(int x, int y,int radius) {
		super();
		rad = radius;
		leftTop = new Point (x,y);
		isCircular = true;
		
		// TODO Auto-generated constructor stub
	}
	@Override
	public void calculatePoints() {
		
		Point RightB;
		RightB = new Point(leftTop.x+(2*rad),leftTop.y+(2*rad));
		points.add(leftTop);
		points.add(RightB);
		
	}
	
	
	public double calculateArea() {
		area = Double.parseDouble(String.format("%.1f",Math.PI*rad*rad));
		return  area;
	}
	
	
	@Override
	public double calculatePerimeter() {
		perimeter = Double.parseDouble(String.format("%.1f",2*Math.PI*rad));
		return perimeter;
		
	}
	@Override
	public void move(int newx, int newy) {
		leftTop = new Point(newx,newy);
		Point RightB;
		RightB = new Point(leftTop.x+(2*rad),leftTop.y+(2*rad));
		points.set(0, leftTop);
		points.set(1, RightB);
		
		
		
		
	}
	@Override
	public String toString() {
		 String point1 = "("+ points.get(0).x +"," + points.get(0).y+")";
		 String point2 = "("+ points.get(1).x +"," + points.get(1).y+")";
		 
		 String point5 = "Points["+point1+point2+"]";
		return "Circle[r="+rad+"]"+"\n"+point5+"\n"+"Area="+area+"\n"+"Perimeter="+perimeter;
	}

}