/*************************************************************************************
	
										Shape Class Test

*************************************************************************************/

//*****************************
//	Test Circle object
//*****************************
var cir = new Circle(new Vector(50, 50), 20);
GraphicsContext.drawCircle(cir, "blue");

//*********************************
//	Test Circle.translate() method
//*********************************
cir.translate(0, 50);
GraphicsContext.drawCircle(cir, "blue");

//*********************************
//	Test Circle.scale() method
//*********************************
cir.scale(5);
GraphicsContext.drawCircle(cir, "blue");