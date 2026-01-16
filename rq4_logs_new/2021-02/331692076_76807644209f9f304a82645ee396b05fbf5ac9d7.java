import java.time.LocalDate;

public class AlwaysOrNeverPreference implements Preference {
    
    int customerOption;

    public AlwaysOrNeverPreference(int customerOption)
    {
        this.customerOption = customerOption;
    }

    public boolean givenDateSendEmail(LocalDate date)
    {
        // Returns true for every date it's compared to
        if (customerOption == 3)
        {
            return true;
        }
        // Never returns true
        else
        {
            return false;
        }
    }
}