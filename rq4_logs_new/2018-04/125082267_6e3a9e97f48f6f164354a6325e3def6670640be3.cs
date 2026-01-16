using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Client.Infrastructure;

namespace Client.ViewModel
{
    class Character_UserControlViewModel: ViewModelBase
    {
        public Character_UserControlViewModel()
        {

        }

        private RelayCommand summonCharacter_Info;

        public RelayCommand SummonCharacter_Info
        {
            get {
                if (summonCharacter_Info == null)
                {
                    summonCharacter_Info = new RelayCommand(ExecuteSummonCharacter_Info);
                }
                return summonCharacter_Info;
            }
        }

        private void ExecuteSummonCharacter_Info(object param)
        {
            WindowViewLoaderService.Show(typeof(CharacterWindowViewModel));
        }
    }
}