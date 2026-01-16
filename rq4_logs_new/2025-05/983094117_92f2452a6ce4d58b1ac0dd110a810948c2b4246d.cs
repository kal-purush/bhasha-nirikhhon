using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using ServiPuntos.uy_mobile.Services.Interfaces;
using ServiPuntos.uy_mobile.Models;
using System.Diagnostics;
using Newtonsoft.Json;
using ServiPuntos.uy_mobile.Models.Enums;


namespace ServiPuntos.uy_mobile.ViewModels;

public partial class IdentityVerificationViewModel(IAuthService authService) : ObservableObject
{
  [ObservableProperty] private string? _documentNumber;
  [ObservableProperty] private string? _serialNumber;

  [RelayCommand]
  private async Task VerifyIdentity()
  {
    if (DocumentNumber == null)
    {
      await Shell.Current.DisplayAlert("Error en la verificación de edad", "Debes ingresar tu numero de cedula y de serie", "OK");
      return;
    }
    try
    {
      var userDTOResponse = await authService.VerifyIdentity(new Document(DocumentNumber, SerialNumber));
      if (userDTOResponse is { Error: false, Data: not null })
      {
        var userJson = JsonConvert.SerializeObject(userDTOResponse.Data);
        await SecureStorage.SetAsync(SecureStorageType.User.ToString(), userJson);
        await Shell.Current.DisplayAlert("Identidad verificada con éxito.", "Podrás acceder a ofertas exclusivas!", "OK");
        await Shell.Current.GoToAsync("..");
      }
      else
      {
        await Shell.Current.DisplayAlert("Error en la verificación de edad", userDTOResponse.Message, "OK");
      }
    }
    catch (Exception ex)
    {
      await Shell.Current.DisplayAlert("Error en la verificación de edad", ex.Message, "OK");
    }
  }

  public void Reset()
  {
    DocumentNumber = null;
    SerialNumber = null;
  }
}