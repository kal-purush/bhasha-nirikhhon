using AutoClicker.WorkWithDll;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AutoClicker.ViewModels
{
    public class FullViewModel : ListenerViewModel
    {
        protected override async Task AsyncExecution()
        {
            throw new NotImplementedException();
        }

        protected override async void ListenerOnKeyPressed(object sender, KeyPressedArgs e)
        {
            if (CanAccessExecution(e.KeyPressed))
            {
                await AsyncExecution();
            }
        }

        protected override void PickingCoordinates()
        {
            throw new NotImplementedException();
        }
    }
}