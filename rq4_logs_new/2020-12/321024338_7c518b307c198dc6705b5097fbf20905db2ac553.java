package rw.col.bookshelf.model.vo;

public class BookshelfCollection {
	private int colBookshelfId;
	private String memberNo;
	private String bookshelfId;
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
	
	
	public String getBookshelfId() {
		return bookshelfId;
	}
	public void setBookshelfId(String bookshelfId) {
		this.bookshelfId = bookshelfId;
	}
	public BookshelfCollection() {
		super();
		// TODO Auto-generated constructor stub
	}
	public BookshelfCollection(int colBookshelfId, String memberNo, String bookshelfId) {
		super();
		this.colBookshelfId = colBookshelfId;
		this.memberNo = memberNo;
		this.bookshelfId = bookshelfId;
	}
	
	
	
}