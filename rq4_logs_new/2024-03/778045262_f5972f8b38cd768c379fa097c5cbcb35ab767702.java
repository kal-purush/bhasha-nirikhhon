import java.util.*;

class Main {
    public static void main(String[] args) {
        lib.clearConsole();
        Random rand = new Random();
        Queue<String> queue1 = new LinkedList<String>();  //LinkedList here makes an empty array compared to PriorityQueue which for some reason limits the size to 11 by default
        Queue<String> queue2 = new LinkedList<String>();  
        Queue<String> queue3 = new LinkedList<String>();  
        Queue<String> queue4 = new LinkedList<String>();  
        Queue[] queueList = {queue1, queue2, queue3, queue4};
        String[] nameList = {"Oliver", "Jake", "Noah", "James",
             	            "Jack" 	,"Connor" ,	"Liam" ,	"John",
             	            "Harry" ,	"Callum" ,	"Mason" 	,"Robert",
             	            "Jacob", 	"Michael", "Charlie", 	"Kyle" ,	"William",
             	            "Thomas" ,	"Joe" 	,"Ethan" ,	"David",
             	            "George" ,	"Reece" 	, 	"Richard",
             	            "Oscar" ,	"Rhys", "Alexander" ,	"Joseph",
             	            "Charles", "Damian" ,	"Daniel"};
        while (true) {
            
            for (Queue i : queueList) {
                System.out.println(i);
            }
            
            //Randomizers
            
            int nameRand = rand.nextInt(nameList.length);
            int enterRand = rand.nextInt(3);
            int exitRand = rand.nextInt(5);
            
            if (enterRand == 1) {
                Queue<String> shortest = queue1;
                for (Queue i : queueList) {
                    if (i.size() < shortest.size()) {
                        shortest = i;
                    }
                }
                shortest.add(nameList[nameRand]);
                System.out.println(nameList[nameRand] + " has joined a queue!");
                lib.sleep(1000);
                
                
            }
            if (exitRand == 1) {
                if (queueList[exitRand].size() > 0) {
                    String exited = (String)(queueList[exitRand].remove());
                    System.out.println(exited + " has left the queue!");
                    lib.sleep(1000);
                }
                
            }
        
            lib.sleep(1000);
            lib.clearConsole();
        }
    }
}