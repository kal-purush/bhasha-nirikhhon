namespace Smelt.Tests.Logic.AddressConverter.ConvertAddress
{
    using Data;
    using Shouldly;
    using Smelt.Logic;
    using Smelt.Logic.AddressConverter;

    public class WhenConvertingHexAndLoromAddresses
    {
        private readonly AppState appState;

        public WhenConvertingHexAndLoromAddresses()
        {
            appState = new AppState { Rom = new Rom() };
            appState.Rom.Header.Makeup.Mapping = Mapping.LoRom;
        }

        [Input("$92    EDF4", "0x096DF4")]
        [Input("0x096DF4", "$92:EDF4")]
        [Input("$86:9243", "0x031243")]
        [Input("0x031243", "$86:9243")]
        [Input("$AC:B600", "0x163600")]
        [Input("0x16s36[00", "$AC:B600")]
        [Input("$A68000", "0x130000")]
        [Input("0x130000", "$A6:8000")]
        [Input("$A60000", "0x130000")]
        [Input("$84AF04", "0x022F04")]
        [Input("0x022F04", "$84:AF04")]
        [Input("$84:8000", "0x020000")]
        [Input("0x020000", "$84:8000")]
        [Input("$fcbaba", "0x3E3ABA")]
        [Input("0x3E3ABA", "$FC:BABA")]
        [Input("$abcdefg", "0x15CDEF")]
        [Input("0x15CDEF", "$AB:CDEF")]
        [Input("0x063600", "$8C:B600")]
        [Input("$8C:B600", "0x063600")]
        [Input("0x8d5b3", "$91:D5B3")]
        [Input("$91:D5B3", "0x08D5B3")]
        public void ShouldConvertAddressesCorrectly(string input, string expected)
        {
            appState.InputAddress = input;
            Command.Run(new ConvertAddressCommand(appState));
            appState.OutputAddress.ShouldBe(expected);
        }
    }
}