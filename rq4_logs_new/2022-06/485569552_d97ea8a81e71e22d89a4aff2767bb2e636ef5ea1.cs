using Siapel.Domain.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Siapel.UI.Services
{
    public class InOutService
    {
        enum EntityType
        {
            StokAwal,
            Pemasukan,
            Transaksi,
            Titipan,
            Ambil
        }
        private List<StokAwal> _stokAwal { get; } = new List<StokAwal>();
        private List<Pemasukan> _pemasukan { get; } = new List<Pemasukan>();
        private List<Transaksi> _transaksi { get; } = new List<Transaksi>();
        private List<TabungBocor> _tabungBocor { get; } = new List<TabungBocor>();
        private DateTimeOffset _tanggal { get; }

        public InOutService(List<StokAwal> stokAwal, List<Pemasukan> pemasukan, List<Transaksi> transaksi, List<TabungBocor> tabungBocor, DateTimeOffset tanggal)
        {
            _stokAwal = stokAwal;
            _pemasukan = pemasukan;
            _transaksi = transaksi;
            _tabungBocor = tabungBocor;
            _tanggal = tanggal;
        }

        private int? GetStokAwalDefault(int? masuk, int? keluar, int? lastStok, int? titipan, int? ambil)
        {
            int? resultStokAwal = lastStok + ambil + keluar - masuk - titipan;
            return resultStokAwal;
        }
        private int? GetSumEntity(string item, DateTimeOffset tanggal, EntityType entityType)
        {
            int? resultSum = 0;
            switch (entityType)
            {
                case EntityType.Pemasukan:
                    if (_pemasukan.Any())
                    {
                        var pemasukanSum = _pemasukan.Where(x => x.Item == item && x.Tanggal == tanggal).Sum(x => x.Jumlah);
                        if (pemasukanSum > 0)
                        {
                            resultSum = pemasukanSum;
                        }
                    }
                    break;
                case EntityType.Transaksi:
                    if (_transaksi.Any())
                    {
                        var penjualanSum = _transaksi.Where(x => x.Item == item && x.Tanggal == tanggal).Sum(x => x.Jumlah);
                        if (penjualanSum > 0)
                        {
                            resultSum = penjualanSum;
                        }
                    }
                    break;
                case EntityType.Titipan:
                    if (_tabungBocor.Any())
                    {
                        var titipanSum = _tabungBocor.Where(x => x.Item == item && x.Tanggal == tanggal).Sum(x => x.Titipan);
                        if (titipanSum > 0)
                        {
                            resultSum = titipanSum;
                        }
                    }
                    break;
                case EntityType.Ambil:
                    if (_tabungBocor.Any())
                    {
                        var titipanSum = _tabungBocor.Where(x => x.Item == item && x.Tanggal == tanggal).Sum(x => x.Ambil);
                        if (titipanSum > 0)
                        {
                            resultSum = titipanSum;
                        }
                    }
                    break;
                default:
                    break;
            }
            return resultSum;
        }
        private int? GetTotalSumEntity(string item, DateTimeOffset tanggal, EntityType entityType)
        {
            int? resultSum = 0;
            switch (entityType)
            {
                case EntityType.Pemasukan:
                    if (_pemasukan.Any())
                    {
                        var pemasukanSum = _pemasukan.Where(x => x.Item == item && x.Tanggal <= tanggal).Sum(x => x.Jumlah);
                        if (pemasukanSum > 0)
                        {
                            resultSum = pemasukanSum;
                        }
                    }
                    break;
                case EntityType.Transaksi:
                    if (_transaksi.Any())
                    {
                        var penjualanSum = _transaksi.Where(x => x.Item == item && x.Tanggal <= tanggal).Sum(x => x.Jumlah);
                        if (penjualanSum > 0)
                        {
                            resultSum = penjualanSum;
                        }
                    }
                    break;
                case EntityType.Titipan:
                    if (_tabungBocor.Any())
                    {
                        var titipanSum = _tabungBocor.Where(x => x.Item == item && x.Tanggal <= tanggal).Sum(x => x.Titipan);
                        if (titipanSum > 0)
                        {
                            resultSum = titipanSum;
                        }
                    }
                    break;
                case EntityType.Ambil:
                    if (_tabungBocor.Any())
                    {
                        var titipanSum = _tabungBocor.Where(x => x.Item == item && x.Tanggal <= tanggal).Sum(x => x.Ambil);
                        if (titipanSum > 0)
                        {
                            resultSum = titipanSum;
                        }
                    }
                    break;
                default:
                    break;
            }
            return resultSum;
        }
        private int? GetLastStokFromTotal(string item, DateTimeOffset tanggal)
        {
            int? result = 0;
            if (item != null)
            {
                int? stokAwal = _stokAwal.FirstOrDefault(x => x.Item == item).Jumlah;
                result = stokAwal + GetTotalSumEntity(item, tanggal, EntityType.Pemasukan) - GetTotalSumEntity(item, tanggal, EntityType.Transaksi) + GetTotalSumEntity(item, tanggal, EntityType.Titipan) - GetTotalSumEntity(item, tanggal, EntityType.Ambil);
            }
            return result;
        }

        public List<object> GetInOutStokList()
        {
            List<object> result = new List<object>();
            string[] items = { "50 KG", "12 KG", "5,5 KG" };
            foreach (var i in items)
            {
                result.Add(new
                {
                    Item = i,
                    StokAkhir = GetLastStokFromTotal(i, _tanggal),
                    TitipanBocor = GetSumEntity(i, _tanggal, EntityType.Titipan),
                    AmbilBocor = GetSumEntity(i, _tanggal, EntityType.Ambil),
                    Penjualan = GetSumEntity(i, _tanggal, EntityType.Transaksi),
                    Masuk = GetSumEntity(i, _tanggal, EntityType.Pemasukan),
                    StokAwal = GetStokAwalDefault(GetSumEntity(i, _tanggal, EntityType.Pemasukan), GetSumEntity(i, _tanggal, EntityType.Transaksi), GetLastStokFromTotal(i, _tanggal), GetSumEntity(i, _tanggal, EntityType.Titipan), GetSumEntity(i, _tanggal, EntityType.Ambil))
                });
            }
            return result;
        }
    }
}