package edu.uco.sharris40.wsp4spencerh.beans;

import edu.uco.sharris40.wsp4spencerh.models.Book;
import edu.uco.sharris40.wsp4spencerh.models.BookItem;
import javax.inject.Named;
import javax.enterprise.context.SessionScoped;
import java.io.Serializable;
import java.util.ArrayList;
import javax.annotation.PostConstruct;
import javax.faces.bean.ManagedProperty;

@Named(value = "shoppingCart")
@SessionScoped
public class ShoppingCart implements Serializable {
  private static final long serialVersionUID = 1L;

  private ArrayList<BookItem> items;

  @ManagedProperty(value="#{bookDatabase}")
  private BookDatabase database;

  public ShoppingCart() {
  }

  @PostConstruct
  public void init() {
    items = new ArrayList<>();
  }

  public ArrayList<BookItem> getItems() {
    return items;
  }

  public void addBook(Book book) {
    if (book == null)
      throw new IllegalArgumentException("book", new NullPointerException());
    boolean found = false;
    for (BookItem item : items) {
      if (item.getBook().equals(book)) {
        found = true;
        item.add(1);
        break;
      }
    }
    if (!found) {
      BookItem newItem = new BookItem();
      newItem.setBook(book);
      items.add(newItem);
    }
  }

  public void addBook(int index) {
    if (database == null)
      throw new NullPointerException("Could not access book database.");
    addBook(database.getBookList().get(index));
  }

  public void removeBook(Book book) {
    if (book == null)
      throw new IllegalArgumentException("book", new NullPointerException());
    boolean found = false;
    for (BookItem item : items) {
      if (item.getBook().equals(book)) {
        found = true;
        item.remove(1);
        break;
      }
    }
    if (!found) {
      BookItem newItem = new BookItem();
      newItem.setBook(book);
      items.add(newItem);
    }
  }

  public void removeBook(int index) {
    if (database == null)
      throw new NullPointerException("Could not access book database.");
    removeBook(database.getBookList().get(index));
  }

}