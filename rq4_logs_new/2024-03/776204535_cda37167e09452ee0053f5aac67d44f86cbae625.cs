using CollectionViewMAUI.Services;

namespace CollectionViewMAUI.Pages;

public partial class RefreshViewCollectionPage : ContentPage
{
	public RefreshViewCollectionPage()
	{
		InitializeComponent();
	}

    async void OnRefresh(System.Object sender, System.EventArgs e)
    {
        //simulamos una operacion que tarde
        //obtener de un Servicio Web
        //o Base de datos
        await Task.Delay(TimeSpan.FromSeconds(1.6));
        collectionView.ItemsSource = ContactService.GetContacts(10);
        refreshView.IsRefreshing = false;
    }
}