/**
 * Created by Leonid on 08.07.2016.
 */
public class ComplexNumber {
    private float real;

    public float getReal() {
        return real;
    }

    public float getImaginary() {
        return imaginary;
    }

    public void setImaginary(float imaginary) {

        this.imaginary = imaginary;
    }

    public void setReal(float real) {
        this.real = real;
    }

    private float imaginary;
    ComplexNumber(float pReal, float pImaginary){
        real = pReal;
        imaginary = pImaginary;
    }
    public ComplexNumber sum(ComplexNumber a, ComplexNumber b){
        return new ComplexNumber(a.getReal() + b.getReal(),
                a.getImaginary() + b.getImaginary());
    }
    public ComplexNumber sub(ComplexNumber a, ComplexNumber b){
        return new ComplexNumber(a.getReal() - b.getReal(),
                a.getImaginary() - b.getImaginary());
    }
    public ComplexNumber mult(ComplexNumber a, ComplexNumber b){
        return new ComplexNumber(a.getReal()*b.getReal() + a.getImaginary()*b.getImaginary(),
                a.getReal()*b.getImaginary() + a.getImaginary()*b.getReal());
    }
}