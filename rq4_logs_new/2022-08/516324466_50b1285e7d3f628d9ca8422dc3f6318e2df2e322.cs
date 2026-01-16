namespace LazyTrader.Binance.Core.Models;

public record NftAsset(
	[property: JsonProperty("total")] int Total,
	[property: JsonProperty("list")] IReadOnlyList<NftToken> Tokens
);