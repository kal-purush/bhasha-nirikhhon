package hu.sol.mik.book.vaadin;

import com.vaadin.data.fieldgroup.BeanFieldGroup;
import com.vaadin.data.fieldgroup.FieldGroup.CommitException;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Table;
import com.vaadin.ui.Table.ColumnGenerator;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Button.ClickListener;

import hu.sol.mik.book.bean.Book;
import hu.sol.mik.book.dao.BookDao;
import hu.sol.mik.book.dao.impl.BookDaoImpl;

public class BookUI extends UI {

	private BeanContainer<Long, Book> bookDataSource;
	private BookDao bookDao;
	protected Window editBookWindow;

	@Override
	protected void init(VaadinRequest request) {
		bookDao = new BookDaoImpl();
		this.setContent(createBookVerticalLayout());
	}

	private Component createBookVerticalLayout() {
		VerticalLayout verticalLayout = new VerticalLayout();
		verticalLayout.setSpacing(true);
		verticalLayout.setMargin(true);
		verticalLayout.addComponent(createBookTable());
		verticalLayout.addComponent(createFunctionLayout());
		return verticalLayout;
	}

	private Component createFunctionLayout() {
		HorizontalLayout horizontalLayout = new HorizontalLayout();
		horizontalLayout.setMargin(true);
		horizontalLayout.setSpacing(true);

		Button newBookButton = new Button("Új könyv felvitel");
		newBookButton.addClickListener(new ClickListener() {

			@Override
			public void buttonClick(ClickEvent event) {
				Book book = new Book();
				openEditWindow(book, "Új könyv felvitel");
			}
		});

		horizontalLayout.addComponent(newBookButton);

		Button deleteSelectedBooksButton = new Button("Kiválasztottak törlése");
		horizontalLayout.addComponent(deleteSelectedBooksButton);
		return horizontalLayout;
	}

	private void openEditWindow(Book book, String title) {
		editBookWindow = new Window(title);
		editBookWindow.setHeight("90%");
		editBookWindow.setWidth("90%");
		editBookWindow.center();
		editBookWindow.setContent(createBookEditLayout(book));
		this.addWindow(editBookWindow);
	}

	protected Component createBookEditLayout(Book book) {
		VerticalLayout verticalLayout = new VerticalLayout();
		final BeanFieldGroup<Book> bookBinder = new BeanFieldGroup<Book>(Book.class);
		bookBinder.setItemDataSource(book);
		verticalLayout.addComponent(createBookEditForm(bookBinder));
		Button saveButton = new Button("Mentés");
		saveButton.addClickListener(new ClickListener() {

			@Override
			public void buttonClick(ClickEvent event) {
				try {
					bookBinder.commit();
					Book bean = bookBinder.getItemDataSource().getBean();
					if (bean.getId() == null) {
						bookDao.saveBook(bean);
					} else {
						bookDao.updateBook(bean);
					}
					editBookWindow.close();
					Notification.show("Sikeres felvitel");
					refreshTable();
				} catch (CommitException e) {
					Notification.show("Hiba történt");
				}
			}
		});
		verticalLayout.addComponent(saveButton);
		return verticalLayout;
	}

	private void refreshTable() {
		bookDataSource.removeAllItems();
		bookDataSource.addAll(bookDao.listAll());
	}

	private Component createBookEditForm(BeanFieldGroup<Book> bookBinder) {
		FormLayout formLayout = new FormLayout();
		TextField titleField = bookBinder.buildAndBind("Cím", "title", TextField.class);
		titleField.setNullRepresentation("");
		formLayout.addComponent(titleField);
		TextField descField = bookBinder.buildAndBind("Leírás", "description", TextField.class);
		descField.setNullRepresentation("");
		formLayout.addComponent(descField);
		TextField authorField = bookBinder.buildAndBind("Szerző", "author", TextField.class);
		authorField.setNullRepresentation("");
		formLayout.addComponent(authorField);
		TextField pubYearField = bookBinder.buildAndBind("Publikáció", "pubYear", TextField.class);
		formLayout.addComponent(pubYearField);
		return formLayout;
	}

	private Component createBookTable() {
		Table table = new Table("Könyvek listája");
		table.setSizeFull();

		bookDataSource = new BeanContainer<Long, Book>(Book.class);
		bookDataSource.setBeanIdProperty("id");
		bookDataSource.addAll(bookDao.listAll());

		table.setContainerDataSource(bookDataSource);
		table.setVisibleColumns("title", "description", "author", "pubYear");
		table.setColumnHeader("pubYear", "asd");
		table.addGeneratedColumn("editBook", new ColumnGenerator() {

			@Override
			public Object generateCell(final Table source, final Object itemId, final Object columnId) {
				Button editButton = new Button("Módosít");
				editButton.addClickListener(new ClickListener() {

					@Override
					public void buttonClick(ClickEvent event) {
						BeanItem<Book> beanItem = (BeanItem<Book>) source.getItem(itemId);
						openEditWindow(beanItem.getBean(), "Könyv szerkesztése");
					}
				});
				return editButton;
			}
		});
		return table;
	}
}