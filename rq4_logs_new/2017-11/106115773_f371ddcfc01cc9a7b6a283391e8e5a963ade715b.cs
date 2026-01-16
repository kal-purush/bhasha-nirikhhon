using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ApiManager.Models
{
    public class SettlementDto
    {
        public string Id { get; set; }
        public SettlementType SettlementType { get; set; }
        public ICollection<Bill> Bills { get; set; }

        public DateTime RequestDate { get; set; }
        public int Installments { get; set; }
        public double Downpayment { get; set; }
        public double MonthlyFee { get { return SubTotal / Installments; } }
        public double SubTotal
        {
            get
            {
                return 1000;
            }
        } //Total mιnus DownPayment


        public SettlementDto()
        {
            Bills = new HashSet<Bill>();
        }

        public SettlementDto(Settlement s)
        {
            Id = s.Id;
            SettlementType = s.SettlementType;
            Bills = s.Bills;
            Installments = s.Installments;
            Downpayment = s.Downpayment;
            RequestDate = s.RequestDate;
        }

        public Settlement ToDomainModel(SettlementDto dto)
        {
            var settlement = new Settlement
            {
                Id = dto.Id,
                Bills = dto.Bills,
                Downpayment = dto.Downpayment,
                Installments = dto.Installments,
                RequestDate = dto.RequestDate,
                SettlementType = dto.SettlementType,
                SettlementTypeId = dto.SettlementType.Id

            };
            return settlement;
        }


    }
}