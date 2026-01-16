package Interface.Student;

public class MainClass {


	
	public static void main(String[] args) {
		
		int intLength = 3;
		int intBreadth = 5;
		int intBase = 4;
		int intHeight = 6;
		
		
		SubRectangle objRect = new SubRectangle();
		objRect.Draw();
		objRect.GetArea(intLength, intBreadth);
		
		SubTriangle objSubTriangle = new SubTriangle();
		objSubTriangle.Draw();
		objSubTriangle.GetArea(intHeight, intBase);
		
	}
		
	
}