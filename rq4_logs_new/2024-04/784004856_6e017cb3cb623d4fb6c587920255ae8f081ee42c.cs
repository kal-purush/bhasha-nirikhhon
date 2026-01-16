using FinalProjectLibraryManagerV01E.Models;
using Microsoft.Maui.Controls;

namespace FinalProjectLibraryManagerV01E.Views;

public partial class BookDetailPage : ContentPage
{
    private Book _selectedBook;

    internal BookDetailPage(Book selectedBook)
    {
        InitializeComponent();

        BindingContext = _selectedBook = selectedBook;
    }

    private async void OnLoanClicked(object sender, EventArgs e)
    {
        bool loanMade = await DisplayAlert("Confirmation", $"Do you want to borrow the book? '{_selectedBook.Title}'?", "Yes", "No");

        if (loanMade)
        {
            await DisplayAlert("Sucess", $"Book '{_selectedBook.Title}' successfully borrowed!", "OK");
        }
    }
}