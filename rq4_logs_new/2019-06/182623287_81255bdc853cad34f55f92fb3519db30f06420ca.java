package movie.beans;

import java.io.Serializable;
import java.sql.Timestamp;

public class MovieReservationBeans implements Serializable {
	private int movieId;
	private int memberNumber;
	private int reservationNumber;
	private String movieName;
	private  int feeBase;
	private int screenNumber;
	private String theaterId;
	private  String termType;
	private Timestamp termStart;
	private Timestamp termFinish;
	private int sheetNumber	;



	public int getSheetNumber() {
		return sheetNumber;
	}
	public void setSheetNumber(int sheetNumber) {
		this.sheetNumber = sheetNumber;
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
	public int getMovieId() {
		return movieId;
	}
	public void setMovieId(int movieId) {
		this.movieId = movieId;
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
	public String getMovieName() {
		return movieName;
	}
	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}
	public int getFeeBase() {
		return feeBase;
	}
	public void setFeeBase(int feeBase) {
		this.feeBase = feeBase;
	}
	public int getScreenNumber() {
		return screenNumber;
	}
	public void setScreenNumber(int screenNumber) {
		this.screenNumber = screenNumber;
	}
	public String getTheaterId() {
		return theaterId;
	}
	public void setTheaterId(String theaterId) {
		this.theaterId = theaterId;
	}
	public String getTermType() {
		return termType;
	}
	public void setTermType(String termType) {
		this.termType = termType;
	}

}