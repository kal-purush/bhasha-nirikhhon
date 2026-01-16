import java.util.ArrayList;

class SelectionSort{

    static IntNode head = null;

    static ArrayList<Integer> sort(ArrayList<Integer> values){

        for(Integer i : values){
            if(head == null){
                IntNode tmp = new IntNode(i.intValue());
                head = tmp;
            }
            else if (i < head.value){
                IntNode tmp = new IntNode(i.intValue());
                tmp.right = head;
                head = tmp;
            }
            else{

                IntNode trav = head;
                IntNode tmp = new IntNode(i.intValue());

                while(trav.right != null && i > trav.right.value){
                    trav = trav.right;
                }
                tmp.right = trav.right;
                trav.right = tmp;

            }

        }
        values.clear();
        IntNode trav = head;
        while(trav != null){
            values.add(trav.value);
            trav = trav.right;
        }

        return values;
    }

    public static void main(String[] args) {
        ReadFile fr = new ReadFile();
		String file = fr.readTestFile(args[0]);
        ArrayList<Integer> values = fr.tokenizeList(file);
        
        ArrayList<Integer> output = sort(values);

        for(Integer i : output){
            System.out.println(i);
        }
        
    }
}