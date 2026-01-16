using System;
using ProtoBuf.Meta;
using packet.protobuff;

namespace packetTank.Serializer
{
    class Program
    {
        static void Main(string[] args)
        {
            var model = TypeModel.Create();
            model.Add(typeof(OutgoingPacket), true);
            model.Add(typeof(IncomigPacket), true);

            model.AllowParseableTypes = true;
            model.AutoAddMissingTypes = true;

            model.Compile("ProtocolSerializer", "ProtocolSerializer.dll");
        }
    }
}