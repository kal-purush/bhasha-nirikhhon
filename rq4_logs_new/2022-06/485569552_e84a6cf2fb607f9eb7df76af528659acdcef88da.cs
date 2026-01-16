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
    public class PemasukanFieldViewModel : ReactiveObject, IRoutableViewModel
    {
        private string _title;
        private Pemasukan _pemasukan;
        public string? UrlPathSegment => _title;
        public IScreen HostScreen { get; }

        public PemasukanFieldViewModel(IScreen screen, string title, Pemasukan pemasukan = null)
        {
            HostScreen = screen;
            _title = title;
            _pemasukan = pemasukan;
            SetField();
            var okEnabled = this.WhenAnyValue(x => x.JumlahLimaPuluh, x => x.JumlahDuaBelas, x => x.JumlahLimaSetengah, (lp, db, ls) => !string.IsNullOrWhiteSpace(lp.ToString()) && !string.IsNullOrWhiteSpace(db.ToString()) && !string.IsNullOrEmpty(ls.ToString()));
            Save = ReactiveCommand.Create(
                () => _pemasukan != null ? EditPemasukan() : new Pemasukan { Tanggal = Tanggal?.Date, InLimaPuluh = JumlahLimaPuluh, InDuaBelas = JumlahDuaBelas, InLimaSetengah = JumlahLimaSetengah }, okEnabled
                );
            Cancel = ReactiveCommand.Create(() => { });
        }

        private DateTimeOffset? _tanggal;
        public DateTimeOffset? Tanggal
        {
            get => _tanggal;
            set => this.RaiseAndSetIfChanged(ref _tanggal, value);
        }

        private int? _jumlahLimaPuluh;
        public int? JumlahLimaPuluh
        {
            get => _jumlahLimaPuluh;
            set => this.RaiseAndSetIfChanged(ref _jumlahLimaPuluh, value);
        }
        private int? _jumlahDuaBelas;
        public int? JumlahDuaBelas
        {
            get => _jumlahDuaBelas;
            set => this.RaiseAndSetIfChanged(ref _jumlahDuaBelas, value);
        }
        private int? _jumlahLimaSetengah;
        public int? JumlahLimaSetengah
        {
            get => _jumlahLimaSetengah;
            set => this.RaiseAndSetIfChanged(ref _jumlahLimaSetengah, value);
        }

        private Pemasukan EditPemasukan()
        {
            _pemasukan.Tanggal = _tanggal;
            _pemasukan.InLimaPuluh = _jumlahLimaPuluh;
            _pemasukan.InDuaBelas = _jumlahDuaBelas;
            _pemasukan.InLimaPuluh = _jumlahLimaSetengah;

            return _pemasukan;
        }

        private void SetField()
        {
            if (_pemasukan != null)
            {
                _tanggal = _pemasukan.Tanggal;
                _jumlahLimaPuluh = _pemasukan.InLimaPuluh;
                _jumlahDuaBelas = _pemasukan.InDuaBelas;
                _jumlahLimaSetengah = _pemasukan.InLimaSetengah;
            }
            else
            {
                _tanggal = DateTimeOffset.Now;
            }
        }

        public ReactiveCommand<Unit, Pemasukan> Save { get; }
        public ReactiveCommand<Unit, Unit> Cancel { get; }
    }
}