package oop.seminar5.task1;

public class CalculateComplex extends Numbers implements Calculating<Numbers> {
    public CalculateComplex(double x, double y) {
        super( x, y );
    }

    @Override
    public Numbers sum() {
        return new Numbers( (x + y), (w + z) );
    }

    @Override
    public Numbers diff() {
        return new Numbers( (x - y), (w - z) );
    }

    @Override
    public Numbers mult() {
        return new Numbers( (x * y - w * z), (x * z + w * y) );
    }

    @Override
    public Numbers div() {
        return new Numbers( (x * y + w * z) / (y * y + z * z), (y * w - x * z) / (y * y + z * z) );
    }
}