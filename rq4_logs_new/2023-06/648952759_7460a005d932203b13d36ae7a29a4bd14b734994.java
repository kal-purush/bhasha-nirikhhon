package lesson4.task4_2;

public class task4_2 {
    public static void main(String[] args) {
        MyArray<String> inData = new MyArray<String>();
        inData.addMyArray("a");
        inData.addMyArray("b");
        inData.addMyArray("c");
        inData.printArray();

        System.out.println();

        MyArray<Integer> inDataInt = new MyArray<Integer>();
        inDataInt.addMyArray(1);
        inDataInt.addMyArray(2);
        inDataInt.addMyArray(3);
        inDataInt.printArray();
    }
}