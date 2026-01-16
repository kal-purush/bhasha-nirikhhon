public class HelloWorld {
    public static void main(String[] args) {
        // Print "Hello, World!" to the console
        System.out.println("Hello, World!");
        
        // Declare and initialize a variable
        int number = 5;
        
        // Perform some calculations
        int result = number * 2;
        
        // Print the result
        System.out.println("The result is: " + result);
        
        // Call a method to display a message
        displayMessage();
    }
    
    public static void displayMessage() {
        System.out.println("This is a custom message.");
    }
}