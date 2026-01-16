import java.util.DoubleSummaryStatistics;

public class SafetySinglePattern {
    private volatile static SafetySinglePattern instance ;
    private SafetySinglePattern(){}

    public SafetySinglePattern getInstance(){
        if ( instance == null ){ //第一次检查
            synchronized (DoubleSummaryStatistics.class){
                if (instance == null ){
                    instance = new SafetySinglePattern() ;
                }
            }
        }
        return instance ;
    }

}