package rw.col.bookshelf.model.vo;

public class BookshelfCollection {
	private int colBookshelfId;
	private String memberNo;
	private String bookselfId;
	public int getColBookshelfId() {
		return colBookshelfId;
	}
	public void setColBookshelfId(int colBookshelfId) {
		this.colBookshelfId = colBookshelfId;
	}
	public String getMemberNo() {
		return memberNo;
	}
	public void setMemberNo(String memberNo) {
		this.memberNo = memberNo;
	}
	public String getBookselfId() {
		return bookselfId;
	}
	public void setBookselfId(String bookselfId) {
		this.bookselfId = bookselfId;
	}
	public BookshelfCollection() {
		super();
		// TODO Auto-generated constructor stub
	}
	public BookshelfCollection(int colBookshelfId, String memberNo, String bookselfId) {
		super();
		this.colBookshelfId = colBookshelfId;
		this.memberNo = memberNo;
		this.bookselfId = bookselfId;
	}
	
	
}