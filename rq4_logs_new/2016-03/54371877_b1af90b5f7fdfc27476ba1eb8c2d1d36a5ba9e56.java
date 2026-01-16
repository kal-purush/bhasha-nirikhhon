package sfa;

/**
 * Created by beiyong14 on 2016/3/23.
 */
public class Client {
    public static void main(String[] args){
        GetNextVisitAdapter adapter = new GetNextVisitAdapter();
        adapter.testApiBySync();
//        adapter.testApiByAsync();
    }
}