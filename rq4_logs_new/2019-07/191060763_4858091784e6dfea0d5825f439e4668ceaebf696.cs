using Newtonsoft.Json;

namespace Lykke.Tools.BlockchainBalancesReport.Clients.NeoScan.Contracts
{
    // ReSharper disable once InconsistentNaming
    public class GetAddressAbstractResponse
    {
        [JsonProperty("total_pages")]
        public int TotalPages { get; set; }

        [JsonProperty("total_entries")]
        public int TotalEntries { get; set; }

        [JsonProperty("page_size")]
        public int PageSize { get; set; }

        [JsonProperty("page_number")]
        public int PageNumber { get; set; }

        [JsonProperty("entries")]
        public Entry[] Entries { get; set; }

        public class Entry
        {
            [JsonProperty("txid")]
            public string Txid { get; set; }

            [JsonProperty("time")]
            public int Time { get; set; }

            [JsonProperty("block_height")]
            public int BlockHeight { get; set; }

            [JsonProperty("asset")]
            public string Asset { get; set; }

            [JsonProperty("amount")]
            public decimal Amount { get; set; }

            [JsonProperty("address_to")]
            public string AddressTo { get; set; }

            [JsonProperty("address_from")]
            public string AddressFrom { get; set; }
        }
    }
}