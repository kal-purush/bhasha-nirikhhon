public class IntervalThread extends Thread{
    private final Sequential sequential;
    private final int left;
    private final int right;
    private final double[] result;

    public IntervalThread(Sequential sequential, int left, int right,double[] result) {
        this.sequential = sequential;
        this.left = left;
        this.right = right;
        this.result = result;
    }

    @Override
    public void run() {

            sequential.computeResultListMatrix(left,right,result);
    }
}