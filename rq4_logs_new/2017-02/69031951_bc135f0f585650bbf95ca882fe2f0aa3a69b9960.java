package base;

public class Ban {
	Integer LibraryCardNumber;
	String Reason, BanDate, ExpirationDate;
	
	public Ban(Integer libraryCardNumber, String reason, String banDate, String expirationDate) {
		super();
		LibraryCardNumber = libraryCardNumber;
		Reason = reason;
		BanDate = banDate;
		ExpirationDate = expirationDate;
	}

	public Integer getLibraryCardNumber() {
		return LibraryCardNumber;
	}

	public void setLibraryCardNumber(Integer libraryCardNumber) {
		LibraryCardNumber = libraryCardNumber;
	}

	public String getReason() {
		return Reason;
	}

	public void setReason(String reason) {
		Reason = reason;
	}

	public String getBanDate() {
		return BanDate;
	}

	public void setBanDate(String banDate) {
		BanDate = banDate;
	}

	public String getExpirationDate() {
		return ExpirationDate;
	}

	public void setExpirationDate(String expirationDate) {
		ExpirationDate = expirationDate;
	}
	
	
}