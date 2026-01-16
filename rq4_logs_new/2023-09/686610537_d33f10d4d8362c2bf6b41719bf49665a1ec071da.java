public  class Circle extends Shape{
    private int radius;
    public Circle(int radius){
        if (radius <= 0  ) {
            throw new IllegalArgumentException("Radius must be higher than 0");
        }
        this.radius=radius;

    }
    @Override
    public double getArea(){
        return Math.PI*radius*radius;
    }
}