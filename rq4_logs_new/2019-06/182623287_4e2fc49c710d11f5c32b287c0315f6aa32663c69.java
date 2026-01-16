package movie.beans;

import java.sql.Timestamp;

public class ReservationBeans {
	int movieId;
	String movieName;
	int memberNumber;
	int reservationNumber;
	int feeBase;
	int sheetNumber;
	String termStrart;
	Timestamp termStart;
	Timestamp termFinish;
	public int getMovieId() {
		return movieId;
	}
	public void setMovieId(int movieId) {
		this.movieId = movieId;
	}
	public String getMovieName() {
		return movieName;
	}
	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}
	public int getMemberNumber() {
		return memberNumber;
	}
	public void setMemberNumber(int memberNumber) {
		this.memberNumber = memberNumber;
	}
	public int getReservationNumber() {
		return reservationNumber;
	}
	public void setReservationNumber(int reservationNumber) {
		this.reservationNumber = reservationNumber;
	}
	public int getFeeBase() {
		return feeBase;
	}
	public void setFeeBase(int feeBase) {
		this.feeBase = feeBase;
	}
	public int getSheetNumber() {
		return sheetNumber;
	}
	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
	}
	public String getTermStrart() {
		return termStrart;
	}
	public void setTermStrart(String termStrart) {
		this.termStrart = termStrart;
	}
	public Timestamp getTermStart() {
		return termStart;
	}
	public void setTermStart(Timestamp termStart) {
		this.termStart = termStart;
	}
	public Timestamp getTermFinish() {
		return termFinish;
	}
	public void setTermFinish(Timestamp termFinish) {
		this.termFinish = termFinish;
	}

}