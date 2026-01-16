
namespace Bcp.Domain.Dtos
{
    public class StockMovementsDto
    {
        public int Id { get; set; }
        public string TmStmp { get; set; }
        public string User { get; set; }
        public string Plant { get; set; }
        public string SapLoc { get; set; }
        public string LabelNr { get; set; }
        public string SerialNr { get; set; }
        public string BatchNr { get; set; }
        public string RackId { get; set; }
        public string RackType { get; set; }
        public string Bin { get; set; }
        public string Product { get; set; }
        public int? Qty { get; set; }
        public string ContentStatus { get; set; }
        public string Delivery { get; set; }
    }
}