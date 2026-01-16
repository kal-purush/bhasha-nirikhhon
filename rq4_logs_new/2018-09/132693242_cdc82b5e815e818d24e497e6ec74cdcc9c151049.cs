using AutoMapper;
using Com.DanLiris.Service.Purchasing.Lib.Models.DailyBankTransaction;
using Com.DanLiris.Service.Purchasing.Lib.ViewModels.DailyBankTransaction;
using System;
using System.Collections.Generic;
using System.Text;

namespace Com.DanLiris.Service.Purchasing.Lib.AutoMapperProfiles
{
    public class DailyBankTransactionProfile : Profile
    {
        public DailyBankTransactionProfile()
        {
            CreateMap<DailyBankTransactionModel, DailyBankTransactionViewModel>()
                .ForPath(d => d.Date, opt => opt.MapFrom(s => s.Date))
                /* Bank */
                .ForPath(d => d.Bank._id, opt => opt.MapFrom(s => s.AccountBankId))
                .ForPath(d => d.Bank.bankCode, opt => opt.MapFrom(s => s.AccountBankCode))
                .ForPath(d => d.Bank.accountName, opt => opt.MapFrom(s => s.AccountBankAccountName))
                .ForPath(d => d.Bank.accountNumber, opt => opt.MapFrom(s => s.AccountBankAccountNumber))
                .ForPath(d => d.Bank.bankName, opt => opt.MapFrom(s => s.AccountBankName))
                .ForPath(d => d.Bank.currency._id, opt => opt.MapFrom(s => s.AccountBankCurrencyId))
                .ForPath(d => d.Bank.currency.code, opt => opt.MapFrom(s => s.AccountBankCurrencyCode))
                .ForPath(d => d.Bank.currency.symbol, opt => opt.MapFrom(s => s.AccountBankCurrencySymbol))

                /* Supplier */
                .ForPath(d => d.Supplier._id, opt => opt.MapFrom(s => s.SupplierId))
                .ForPath(d => d.Supplier.code, opt => opt.MapFrom(s => s.SupplierCode))
                .ForPath(d => d.Supplier.name, opt => opt.MapFrom(s => s.SupplierName))

                /* Buyer */
                .ForPath(d => d.Buyer._id, opt => opt.MapFrom(s => s.BuyerId))
                .ForPath(d => d.Buyer.code, opt => opt.MapFrom(s => s.BuyerCode))
                .ForPath(d => d.Buyer.name, opt => opt.MapFrom(s => s.BuyerName))

                .ReverseMap();
        }
    }
}