package ShapesDrawing;


public class Circle implements Drawable{
    private int radius;
    private int centerX;
    private int centerY;
    public Circle (int radius, int centerX, int centerY){
        this.setRadius(radius);
        this.setCenterX(centerX);
        this.setCenterY(centerY);
    }

    public int getRadius() {
        return this.radius;
    }

    private void setRadius(int radius) {
        this.radius = radius;
    }

    public int getCenterX() {
        return this.centerX;
    }

    private void setCenterX(int centerX) {
        this.centerX = centerX;
    }

    public int getCenterY() {
        return this.centerY;
    }

    private void setCenterY(int centerY) {
        this.centerY = centerY;
    }

    @Override
    public void draw(){
        double r_in = this.getRadius() - 0.4;
        double r_out = this.getRadius() + 0.4;

        for(double y = this.getRadius(); y >= -this.getRadius(); --y){
            for(double x = -this.getRadius(); x < r_out; x += 0.5){
                double value = x * x + y * y;
                if(value >= r_in * r_in && value <= r_out * r_out){
                    System.out.print("*");
                } else {
                    System.out.print(" ");
                }
            }
            System.out.println();
        }
    }
}