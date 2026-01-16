using ReactiveUI;
using Siapel.Domain.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading.Tasks;

namespace Siapel.UI.ViewModels.DialogViewModels
{
    public class StokAwalFieldViewModel : ReactiveObject, IRoutableViewModel
    {
        private string _title;
        private List<string> _itemList;
        private StokAwal _stokAwal;
        public string? UrlPathSegment => _title;
        public IScreen HostScreen { get; }
        public List<string> ItemList => _itemList;

        public StokAwalFieldViewModel(IScreen screen, string title, StokAwal stokAwal)
        {
            HostScreen = screen;
            _title = title;
            _stokAwal = stokAwal;
            _itemList = new List<string>() { "50 KG", "12 KG", "5,5 KG" };
            SetField();
            var okEnabled = this.WhenAnyValue(x => x.SelectedItem, x => x.Jumlah, (i, j) => !string.IsNullOrWhiteSpace(i) && !string.IsNullOrWhiteSpace(j.ToString()));
            Save = ReactiveCommand.Create(
                () => EditStokAwal(), okEnabled
                );

            Cancel = ReactiveCommand.Create(() => { });
        }

        private string _selectedItem;
        public string SelectedItem
        {
            get => _selectedItem;
            set => this.RaiseAndSetIfChanged(ref _selectedItem, value);
        }

        private int? _jumlah;
        public int? Jumlah
        {
            get => _jumlah;
            set => this.RaiseAndSetIfChanged(ref _jumlah, value);
        }
        private StokAwal EditStokAwal()
        {
            _stokAwal.Item = _selectedItem;
            _stokAwal.Jumlah = _jumlah;
            return _stokAwal;
        }
        private void SetField()
        {
            if (_stokAwal != null)
            {
                _selectedItem = _stokAwal.Item;
                _jumlah = _stokAwal.Jumlah;
            }            
        }
        public ReactiveCommand<Unit, StokAwal> Save { get; }
        public ReactiveCommand<Unit, Unit> Cancel { get; }
    }
}