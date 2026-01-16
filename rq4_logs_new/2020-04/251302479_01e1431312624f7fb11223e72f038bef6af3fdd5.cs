using CoinHybridApp.Service;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Acr.UserDialogs;
using System.Threading.Tasks;


namespace CoinHybridApp.Service
{
   
    public class ConfirmationService : IConfirmationService
    {
        public async Task<bool> AskConfirmation(string message)
        {
            var config = new ConfirmConfig()
            {
                Title = "Titre",
                Message = message,
                OkText = "Oui",
                CancelText = "Non",
            };
            return await UserDialogs.Instance.ConfirmAsync(config);
        }
    }
}