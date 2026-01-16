public class BmiService {
    public int calculate(int massa, int height) {
        int BMI = massa * 10000 / (height * height);
        return BMI;
    }
}