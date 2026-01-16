using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Tools.BlockchainBalancesReport.Clients.Samurai;
using Lykke.Tools.BlockchainBalancesReport.Configuration;
using Microsoft.Extensions.Options;

namespace Lykke.Tools.BlockchainBalancesReport.Blockchains.Ethereum
{
    public class EthereumBalanceProvider : IBalanceProvider
    {
        public string BlockchainType => "Ethereum";

        private readonly SamuraiClient _client;
        private readonly Dictionary<string, SamuraiErc20TokenResponse> _tokensCache;

        public EthereumBalanceProvider(IOptions<EthereumSettings> settings)
        {
            _client = new SamuraiClient(settings.Value.SamuraiApiUrl);
            _tokensCache = new Dictionary<string, SamuraiErc20TokenResponse>();
        }

        public async Task<IReadOnlyDictionary<Asset, decimal>> GetBalancesAsync(string address, DateTime at)
        {
            var atTime = new DateTimeOffset(at, TimeSpan.Zero).ToUnixTimeSeconds();
            var normalizedAddress = address.ToLower();
            var ethBalance = await GetEthBalanceAsync(normalizedAddress, atTime);
            var erc20Balances = await GetErc20BalancesAsync(normalizedAddress, atTime);

            return erc20Balances
                .Concat
                (
                    new[]
                    {
                        new KeyValuePair<Asset, decimal>(new Asset("ETH", "ETH", "ETH"), ethBalance)
                    }
                )
                .ToDictionary(x => x.Key, x => x.Value);
        }

        private async Task<decimal> GetEthBalanceAsync(string address, long at)
        {
            var balance = 0M;
            var start = 0;
            const int count = 500;
            
            do
            {
                var response = await _client.GetOperationsHistoryAsync(address, start, count);

                foreach (var operation in response.History)
                {
                    if (operation.BlockTimestamp > at)
                    {
                        continue;
                    }

                    var value = decimal.Parse(operation.Value) * 0.000000000000000001M;

                    if (address.Equals(operation.From, StringComparison.InvariantCultureIgnoreCase))
                    {
                        var gasPrice = long.Parse(operation.GasPrice) * 0.000000000000000001M;
                        var gasUsed = long.Parse(operation.GasUsed);
                        var fee = gasPrice * gasUsed;

                        balance -= fee;

                        if (!operation.HasError)
                        {
                            balance -= value;
                        }
                    }
                    else if(!operation.HasError)
                    {
                        balance += value;
                    }
                }

                if (response.History.Count < count)
                {
                    break;
                }

                start += response.History.Count;
            }
            while (true);
            
            return balance;
        }

        private async Task<IReadOnlyDictionary<Asset, decimal>> GetErc20BalancesAsync(string address, long at)
        {
            var balances = new Dictionary<(string Address, string Name), decimal>();
            var start = 0;
            const int count = 1000;

            do
            {
                var operations = await _client.GetErc20OperationsHistory(address, start, count);

                foreach (var operation in operations)
                {
                    if (operation.BlockTimestamp > at)
                    {
                        continue;
                    }

                    var token = await GetErc20TokenAsync(operation.Contract);

                    if (token == null)
                    {
                        continue;
                    }

                    var multiplier = (decimal)Math.Pow(10, -token.Decimals);
                    var value = decimal.Parse(operation.TransferAmount) * multiplier;
                    var balanceChange = address.Equals
                        (operation.From, StringComparison.InvariantCultureIgnoreCase)
                        ? -value
                        : value;

                    var contract = (operation.Contract, token.Symbol);

                    if (balances.TryGetValue(contract, out var balance))
                    {
                        balances[contract] = balance + balanceChange;
                    }
                    else
                    {
                        balances.Add(contract, balanceChange);
                    }
                }

                if (operations.Count < count)
                {
                    break;
                }

                start += operations.Count;
            }
            while (true);

            return balances.ToDictionary(x => GetAsset(x.Key.Address, x.Key.Name), x => x.Value);
        }

        private async Task<SamuraiErc20TokenResponse> GetErc20TokenAsync(string contractAddress)
        {
            if (_tokensCache.TryGetValue(contractAddress, out var token))
            {
                return token;
            }

            token = await _client.GetErc20Token(contractAddress);

            _tokensCache.Add(contractAddress, token);

            return token;
        }

        private static Asset GetAsset(string contractAddress, string name)
        {
            // TODO: Get Lykke Asset ID
            return new Asset(name, contractAddress, null);
        }
    }
}