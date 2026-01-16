package kpi.zaranik.element.observer;

public class RejectionObserver implements Observer {

    private double summaryRejectionProbability;
    private int observationsNumber;

    public void observeRejectionProbability(int nowInQueue, int goingToLeaveQueue) {
        if (nowInQueue > 0) {
            summaryRejectionProbability += (nowInQueue - goingToLeaveQueue + 0.) / nowInQueue * 100;
            observationsNumber++;
        }
    }

    @Override
    public void printResult() {
        System.out.println("Rejection probability: " + summaryRejectionProbability / observationsNumber * 100 + "%");
    }
}