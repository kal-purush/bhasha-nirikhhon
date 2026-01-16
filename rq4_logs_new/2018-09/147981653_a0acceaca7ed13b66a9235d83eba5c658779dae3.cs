using System;

namespace Project1
{
    class Rectangle1 
    {
        int width; 
        int height;
        
        public Rectangle1(int inputWidth, int inputHeight)
        {
            width = inputWidth;
            height = inputHeight;    
        } 
        /* calulate area */
        public int calculateArea()
        {
        return width * height;
        }
        /* calcuate perimeter */
        public int calculatePerimeter()
        {
         return width + width + height + height;
        }
   
    class Square1 : Rectangle1
    {
        public Square1(int inputWidth, int inputHeight) : base (inputWidth, inputHeight)
        {
        calculateArea();
        calculatePerimeter();
        }
    }   


    static void Main ( string[ ] args )
    {
       //* input square */
        Square1 square = new Square1(inputWidth, );
        Console.WriteLine ("The area of the square is {0}", square.calculateArea());
        Console.WriteLine ("The perimater of the square is {0}", square.calculatePerimeter());

        //* input rectangle */
        Rectangle1 rectangle = new Rectangle1(150, 50);
        Console.WriteLine ("The area of the rectangle is {0}", rectangle.calculateArea());
        Console.WriteLine ("The perimater of the rectangle is {0}", rectangle.calculatePerimeter());
        
    }
   }

}