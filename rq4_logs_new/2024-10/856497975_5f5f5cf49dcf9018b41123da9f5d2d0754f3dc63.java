package videoStore;

public class EnglishStatement extends Statement{

    protected String header(Customer c) {
        return "Rental Record for " + c.getName() + "\n";
    }

    protected String rentalLine(Rental each) {
        return "\t" + each.getMovie().getTitle() + "\t" + each.getAmount() + "\n";
    }


    protected String footer(Customer c) {
        return "Amount owed is " + c.getTotalAmount() + "\n"
                + "You earned " + c.getFrequentRenterPoints() + " frequent renter points\n";
    }
}