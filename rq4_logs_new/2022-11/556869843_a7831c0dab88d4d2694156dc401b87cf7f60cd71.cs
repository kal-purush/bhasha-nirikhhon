public interface ControllerInterface {
    public void setFFTvalues(int high, int mid, int low);
}

public class FFTcontroller : ControllerInterface {
    private FFTmodel model;
    FFTview view;
    public void setFFTvalues(int high, int mid, int low) {
        model.setFFTvalues(high, mid, low);
    }
}