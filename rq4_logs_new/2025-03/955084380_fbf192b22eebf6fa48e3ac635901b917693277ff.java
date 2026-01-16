import java.util.Scanner;

class Book {
    private String title;
    private String author;
    private double price;
    
    public Book(String title, String author, double price) {
        this.title = title;
        this.author = author;
        this.price = price;
    }
    
    public String getTitle() {
        return title;
    }
    
    public String getAuthor() {
        return author;
    }
    
    public double getPrice() {
        return price;
    }
    
    public void displayBookDetails() {
        System.out.println("Title: " + title);
        System.out.println("Author: " + author);
        System.out.println("Price: " + price);
    }
}

public class LibraryBookManagement {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        
        Book[] books = new Book[5];
        
        for (int i = 0; i < 5; i++) {
            System.out.println("Enter details for Book " + (i+1) + ":");
            System.out.print("Enter Book Title: ");
            String title = scanner.nextLine();
            System.out.print("Enter Book Author: ");
            String author = scanner.nextLine();
            System.out.print("Enter Book Price: ");
            double price = scanner.nextDouble();
            scanner.nextLine();
            
            books[i] = new Book(title, author, price);
        }
        
        System.out.print("Enter the title of the book you want to search for: ");
        String searchTitle = scanner.nextLine();
        
        boolean found = false;
        for (Book book : books) {
            if (book.getTitle().equalsIgnoreCase(searchTitle)) {
                System.out.println("\nBook Found!");
                book.displayBookDetails();
                found = true;
                break;
            }
        }
        
        if (!found) {
            System.out.println("Book not found!");
        }
    }
}