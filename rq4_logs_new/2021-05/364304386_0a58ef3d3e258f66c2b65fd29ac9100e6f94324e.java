package DPP_Time;

public interface Subject {
    void registerObserver(TimeObserver timeobserver);
    void unregisterObserver(TimeObserver timeobserver);
    void updateObserver();
}