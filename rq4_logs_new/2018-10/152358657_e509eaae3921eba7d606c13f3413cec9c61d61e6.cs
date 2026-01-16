namespace Smelt.Tests.Logic.AddressConverter.ConvertAddress
{
    using Data;
    using Shouldly;
    using Smelt.Logic;
    using Smelt.Logic.AddressConverter;

    public class WhenConvertingHexAndHiromAddresses
    {
        private readonly AppState appState;

        public WhenConvertingHexAndHiromAddresses()
        {
            appState = new AppState { Rom = new Rom() };
            appState.Rom.Header.Makeup.Mapping = Mapping.HiRom;
        }

        [Input("$D2    EDF4", "0x12EDF4")]
        [Input("0x12EDF4", "$D2:EDF4")]
        [Input("$C6:9243", "0x069243")]
        [Input("0x069243", "$C6:9243")]
        [Input("$D6:3600", "0x163600")]
        [Input("0x16s36[00", "$D6:3600")]
        [Input("$D38000", "0x138000")]
        [Input("0x138000", "$D3:8000")]
        [Input("$D30000", "0x130000")]
        [Input("0x130000", "$D3:0000")]
        [Input("$C22F04", "0x022F04")]
        [Input("0x022F04", "$C2:2F04")]
        [Input("$C4:8000", "0x048000")]
        [Input("0x048000", "$C4:8000")]
        [Input("$fcbaba", "0x3CBABA")]
        [Input("0x3CBABA", "$FC:BABA")]
        [Input("$abcdefg", "0x2BCDEF")]
        [Input("0x2BCDEF", "$EB:CDEF")]
        [Input("0x063600", "$C6:3600")]
        [Input("$C6:3600", "0x063600")]
        [Input("0x8d5b3", "$C8:D5B3")]
        [Input("$C8:D5B3", "0x08D5B3")]
        public void ShouldConvertAddressesCorrectly(string input, string expected)
        {
            appState.InputAddress = input;
            Command.Run(new ConvertAddressCommand(appState));
            appState.OutputAddress.ShouldBe(expected);
        }
    }
}