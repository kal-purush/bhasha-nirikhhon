public class Main {

    public static void main(String[] args) {
        Stack stack = new Stack(3);

        System.out.println("stack must be empty but is empty:" + stack.isEmpty());
        System.out.println("stack size must be 0 and  is: " + stack.size());

        stack.push(10);
        System.out.println("stack must not be empty but is empty:" + stack.isEmpty());
        System.out.println("stack size must be 1 and  is: " + stack.size());

        stack.push(20);
        System.out.println("stack must not be empty but is empty:" + stack.isEmpty());
        System.out.println("stack size must be 2 and  is: " + stack.size());

        stack.push(30);
        System.out.println("stack must not be empty but is empty:" + stack.isEmpty());
        System.out.println("stack size must be 3 and  is: " + stack.size());

        try {
            stack.push(4);
            System.out.println("We must not get that line in the logs");
        } catch (IllegalStateException e) {
            System.out.println("Exception text must be 'overflow' and text is = " + e.getMessage());
        }

        System.out.println("Pop value 30 = " + stack.pop());
        System.out.println("stack must not be empty but is empty:" + stack.isEmpty());

        System.out.println("Pop value 20 = " + stack.pop());
        System.out.println("stack must not be empty but is empty:" + stack.isEmpty());

        System.out.println("Pop value 10 = " + stack.pop());
        System.out.println("stack must be empty but is empty:" + stack.isEmpty());

        try {
            int value = stack.pop();
            System.out.println("We must not get that line in the logs");
        } catch (IllegalStateException e) {
            System.out.println("Exception text must be 'underflow' and text is = " + e.getMessage());
        }
    }
}