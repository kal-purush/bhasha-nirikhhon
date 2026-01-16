using Lykke.AzureStorage.Tables;

namespace Lykke.Service.Bitcoin.Api.AzureRepositories.Wallet
{
    public class LastProcessedBlockEntity:AzureTableEntity
    {
        public static string GeneratePartionKey()
        {
            return "_";
        }
        
        public static string GenerateRowKey()
        {
            return "_";
        }

        public int Height { get; set; }


        public static LastProcessedBlockEntity Create(int height)
        {
            return new LastProcessedBlockEntity
            {
                Height = height,
                PartitionKey = GeneratePartionKey(),
                RowKey = GenerateRowKey()
            };
        }
    }
}