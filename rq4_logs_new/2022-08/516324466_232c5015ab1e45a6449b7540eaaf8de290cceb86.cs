namespace LazyTrader.Binance.Core.Models;

public record ApiKeyPermission(
	[property:JsonProperty("ipRestrict")] bool IpRestrict,
	[property:JsonProperty("createTime")] long CreateTime,
	[property:JsonProperty("enableWithdrawals")] bool EnableWithdrawals,
	[property:JsonProperty("enableInternalTransfer")] bool EnableInternalTransfer,
	[property:JsonProperty("permitsUniversalTransfer")] bool PermitsUniversalTransfer,
	[property:JsonProperty("enableVanillaOptions")] bool EnableVanillaOptions,
	[property:JsonProperty("enableReading")] bool EnableReading,
	[property:JsonProperty("enableFutures")] bool EnableFutures,
	[property:JsonProperty("enableMargin")] bool EnableMargin,
	[property:JsonProperty("enableSpotAndMarginTrading")] bool EnableSpotAndMarginTrading,
	[property:JsonProperty("tradingAuthorityExpirationTime")] long TradingAuthorityExpirationTime
);