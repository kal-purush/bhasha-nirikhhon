import java.util.*;

class Solution {
    LinkedList<Integer> container = new LinkedList<>();
    LinkedList<Integer> orderList = new LinkedList<>();
    public int solution(int[] order) {
        //init
        for(int i=0; i<order.length; i++){
            orderList.addLast(order[i]);
        }
        int answer = 0;
        int originNum = 1;
        int containerNum = 0;
        int orderNum = 0;
        
        while(true){
            orderNum = (orderList.isEmpty()) ? 0 : orderList.getFirst();
            containerNum = (container.isEmpty()) ? 0 : container.getLast();
            
            if(orderList.isEmpty() || (containerNum != orderNum && originNum > order.length)){
                break;
            }

            if(containerNum == orderNum){
                container.removeLast();
                orderList.removeFirst();
                answer++;
            }else if(orderNum == originNum){
                orderList.removeFirst();
                originNum++;
                answer++;
            }else{
                container.addLast(originNum++);
            }
        }
        
        return answer;
    }
}