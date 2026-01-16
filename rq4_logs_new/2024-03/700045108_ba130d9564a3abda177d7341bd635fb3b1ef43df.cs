namespace ManagementApi.Constants
{
    public static class ProgramHelperStrings
    {
        public const string S = "s";
        public const string SALE = "sale";
        public const string L = "l";
        public const string LOT = "lot";
        public const string EX = "ex";
        public const string EXIT = "exit";
    }

    public static class SOAPRequestHelperStrings
    {
        public const string SALE_TYPE = "TimeBased";
        public const string LOT_END_TIME_INTERVAL = "PT2M";
    }

    public static class SQLQueries
    {
        public const string AUCTION_DETAILS_QUERY = @"SELECT
                                                          [SaleName]
	                                                      ,[SaleNumber]
	                                                      ,[StatusCode] AS SaleStatus
	                                                      ,[SaleTypeCode] AS SaleType
	                                                      ,[SalesTaxExemptionEnabled]
	                                                      ,[EmailAddress] AS SaleEmail
	                                                      ,[HasInBondLots] AS SaleHasInBondLots
	                                                      ,[SaleCoordinatorEmailSignatureHtml] AS SaleCoordinatorEmailSignature
	                                                      ,[LotEndTimeInterval]
	                                                      ,[KycMandatory]
	                                                      ,bis.[Name] AS BidIncrementSet
	                                                      ,[BondedDeliveryDisabled]
	                                                      ,[BuyersPremiumOverride]
	                                                      ,bus.[Code] AS CompanyCode
	                                                      --CompanyCode
	                                                      ,cur.[ISO_4217_Code] AS Currency
	                                                      ,[DefaultCreditLimit]
	                                                      ,[AnalyticsSaleType]
	                                                      ,[Starts] AS SaleStartTime
	                                                      ,[Ends] AS SaleEndTime
	                                                      ,aes.[SessionStart] AS SessionStartTime
	                                                      --SessionStart
	                                                      ,aes.[SessionEnd] AS SessionEndTime
	                                                      --SessionEnd
	                                                      ,[ShippingConfigurationType]
	                                                      --OffsetMinutes
	                                                      ,jgrsa.[AttributeName] AS GeoRestrictionAttribute
	                                                      --GeoRestrictionAttribute
                                                      FROM [dbo].[AuctionEvents] ae
                                                      Join [dbo].[Businesses] bus ON ae.[BusinessId] = bus.[Id]
                                                      Join [dbo].[AuctionEventSessions] aes ON ae.[Id] = aes.[AuctionEventId]
                                                      Left Join [dbo].[JdeGeoRestrictionSaleAttributes] jgrsa ON ae.[Id] = jgrsa.[AuctionEventId]
                                                      Join [dbo].[Currencies] cur ON ae.[CurrencyId] = cur.[Id]
                                                      Join [dbo].[BidIncrementSets] bis ON ae.[BidIncrementSetId] = bis.[Id]
                                                      Where ae.[Id] = @SaleId";

        public const string ITEM_DETAILS_QUERY = @"SELECT
														it.[ExplicitContent],
														it.[Fragile],
														it.[HubId],
														it.[ItemType],
														it.[ParentLotNumber] AS LotNumber,
														ae.[SaleNumber],
														ud.[UniqueIdentifier],
														it.[AuctionEventId],
														it.[ArtistMaker],
														it.[ConditionReport],
														it.[Description],
														it.[Engraved],
														it.[Exhibited],
														it.[ExtraInformation],
														it.[Literature],
														it.[LotEssay],
														it.[PostLotText],
														it.[PreLotText],
														it.[Provenance],
														it.[Title]
													FROM
														[dbo].[Items] it
														LEFT JOIN [AuctionEvents] ae ON ae.Id = [AuctionEventId] OUTER APPLY (

															SELECT
																CONCAT(ae.[SaleNumber], '.', it.[ParentLotNumber]) AS UniqueIdentifier
														) AS ud
													WHERE
														it.[Id] = @ItemId";

        public const string ITEM_ENG_TRANSLATIONS_QUERY = @"SELECT
																[ArtistMaker],
																[ConditionReport],
																[Description],
																[Engraved],
																[Exhibited],
																[ExtraInformation],
																[Literature],
																[LotEssay],
																[PostLotText],
																[PreLotText],
																[Provenance],
																[Title]
															FROM
																[dbo].[Items]
															WHERE
																[Id] = @ItemId";

        public const string ITEM_TRANSLATIONS_QUERY = @"SELECT
															[ArtistMaker],
															[ConditionReport],
															[Description],
															[Engraved],
															[Exhibited],
															[ExtraInformation],
															[CultureCode] AS LanguageCode,
															[Literature],
															[LotEssay],
															[PostLotText],
															[PreLotText],
															[Provenance],
															[Title]
														FROM
															[dbo].[ItemTranslations]
														WHERE
															ItemId = @ItemId";
    }
}