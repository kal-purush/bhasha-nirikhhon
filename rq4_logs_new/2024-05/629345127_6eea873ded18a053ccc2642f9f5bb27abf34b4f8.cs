namespace Portkey.Indexer.CA.Entities;

public class TokenTransferInfo
{
    public TokenInfoIndex TokenInfo { get; set; }
    
    public NFTInfoIndex NftInfo { get; set; }

    public TransferInfo TransferInfo { get; set; }
}