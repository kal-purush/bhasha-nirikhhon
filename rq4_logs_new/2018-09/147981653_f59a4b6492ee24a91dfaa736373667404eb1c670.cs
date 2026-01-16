using System;

namespace Project1
{
    class Rectangle1 
    {
        int shapeWidth; 
        int shapeHeight;

        public Rectangle1(int width, int height)
        {
            shapeWidth = width;
            shapeHeight = height;
        } 
        public int area()
        {
        return shapeWidth * shapeHeight;
        }
        public int perimeter()
        {
         return shapeWidth + shapeWidth + shapeHeight + shapeHeight;
        }
   
    class Square1 : Rectangle1
    {
        public Square1(int width, int height) : base (width, height)
        {
        area();
        perimeter();
        }
    }   

    static void Test ( string[ ] args )
    {
       //* test square */
        Square1 square = new Square1(50, 50);
        Console.WriteLine ("The area of the square is {0}", square.area());
        Console.WriteLine ("The perimater of the square is {0}", square.perimeter());

        //* test rectangle */
        Rectangle1 rectangle = new Rectangle1(150, 50);
        Console.WriteLine ("The area of the rectangle is {0}", rectangle.area());
        Console.WriteLine ("The perimater of the rectangle is {0}", rectangle.perimeter());
        

         
    
    }
   }

}