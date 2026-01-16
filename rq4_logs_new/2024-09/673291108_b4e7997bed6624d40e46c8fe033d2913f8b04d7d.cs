using System.Threading.Tasks;
using AElf;
using AElf.Types;
using AwakenServer.Activity.Dtos;
using AwakenServer.Grains.Tests;
using AwakenServer.Trade;
using Nethereum.Hex.HexConvertors.Extensions;
using Shouldly;
using Xunit;
using CryptoHelper = AElf.Cryptography.CryptoHelper;

namespace AwakenServer.Activity;

[Collection(ClusterCollection.Name)]
public class ActivityAppServiceTests : TradeTestBase
{
    private readonly IActivityAppService _activityAppService;

    public ActivityAppServiceTests()
    {
        _activityAppService = GetRequiredService<IActivityAppService>();
    }

    [Fact]
    public async Task JoinTest()
    {
        var address = Address.FromPublicKey("AAA".HexToByteArray());
        var message = "Join";
        var keyPair = CryptoHelper.GenerateKeyPair();
        var sign = CryptoHelper.SignWithPrivateKey(keyPair.PrivateKey, HashHelper.ComputeFrom(message).ToByteArray());
        
        var joinStatusDto = await _activityAppService.GetJoinStatusAsync(new GetJoinStatusInput()
        {
            ActivityId = 1,
            Address = address.ToBase58()
        });
        joinStatusDto.Status.ShouldBe(0);
        joinStatusDto.NumberOfJoin.ShouldBe(0);

        await _activityAppService.JoinAsync(new JoinInput()
        {
            Message = message,
            Signature = ByteExtensions.ToHex(sign),
            PublicKey = ByteExtensions.ToHex(keyPair.PublicKey),
            Address = address.ToBase58(),
            ActivityId = 1
        });
        await Task.Delay(3000);
        joinStatusDto = await _activityAppService.GetJoinStatusAsync(new GetJoinStatusInput()
        {
            ActivityId = 1,
            Address = address.ToBase58()
        });
        joinStatusDto.Status.ShouldBe(1);
        joinStatusDto.NumberOfJoin.ShouldBe(1);
    }

}