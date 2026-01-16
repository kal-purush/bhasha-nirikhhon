public record BestItem(long id, String name, String description) {

    void someMethod() {
        System.out.print("What a method in the record?!");
    }
}