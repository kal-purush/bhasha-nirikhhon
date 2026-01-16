namespace FinalProjectLibraryManagerV01E.Views;

public partial class SearchSelectUser : ContentPage
{
	public SearchSelectUser()
	{
		InitializeComponent();
	}

    private void homeSearchUserBtn_Clicked(object sender, EventArgs e)
    {
		Shell.Current.GoToAsync(nameof(HomepageCustomer));
    }

    private void YourReservationSearchUserBtn_Clicked(object sender, EventArgs e)
    {
        Shell.Current.GoToAsync(nameof(YourReservationsUser));
    }

    private void LoansSearchUserBtn_Clicked(object sender, EventArgs e)
    {
        Shell.Current.GoToAsync(nameof(YourLoansUser));
    }

    private void FinesPaymentsSearchUserBtn_Clicked(object sender, EventArgs e)
    {
        Shell.Current.GoToAsync(nameof(YourFinesPaymentsUser));
    }
}