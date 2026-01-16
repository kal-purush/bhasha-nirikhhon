package lib;

import java.time.LocalDate;

public class EntryJob extends Passport{
    //EntryJob
	private LocalDate entryDate;
	private int monthWorkingInYear;

    /**
     * @return LocalDate return the entryDate
     */
    public LocalDate getEntryDate() {
        return entryDate;
    }

    /**
     * @param entryDate the entryDate to set
     */
    public void setEntryDate(LocalDate entryDate) {
        this.entryDate = entryDate;
    }

    /**
     * @return int return the monthWorkingInYear
     */
    public int getMonthWorkingInYear() {
        return monthWorkingInYear;
    }

    /**
     * @param monthWorkingInYear the monthWorkingInYear to set
     */
    public void setMonthWorkingInYear(int monthWorkingInYear) {
        this.monthWorkingInYear = monthWorkingInYear;
    }

}