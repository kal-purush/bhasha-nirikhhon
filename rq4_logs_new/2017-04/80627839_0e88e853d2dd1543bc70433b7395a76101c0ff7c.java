package app.web.dto;

/**
 * Created by Gabriel on 11.04.2017.
 */
public class ShoppingCartItem {
    private int bookid;
    private String bookTitle;
    private int quantity;

    public ShoppingCartItem(int bookid, String bookTitle, int quantity) {
        this.bookid = bookid;
        this.bookTitle = bookTitle;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "book id: " + bookid + ", bookTitle: " + bookTitle + ", quantity: " + quantity;
    }

    public int getBookid() {
        return bookid;
    }

    public void setBookid(int bookid) {
        this.bookid = bookid;
    }

    public String getBookTitle() {
        return bookTitle;
    }

    public void setBookTitle(String bookTitle) {
        this.bookTitle = bookTitle;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShoppingCartItem item = (ShoppingCartItem) o;

        if (bookid != item.bookid) return false;
        if (quantity != item.quantity) return false;
        return bookTitle != null ? bookTitle.equals(item.bookTitle) : item.bookTitle == null;
    }

    @Override
    public int hashCode() {
        int result = bookid;
        result = 31 * result + (bookTitle != null ? bookTitle.hashCode() : 0);
        result = 31 * result + quantity;
        return result;
    }
}