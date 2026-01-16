using System.Text;

namespace AocNetLib
{
    public class Day16
    {
        public string Solve(string input)
        {
            var packet = ParseInput(input);
            int result = packet.SumVersions();
            return result.ToString();
        }

        public string Solve2(string input)
        {
            var packet = ParseInput(input);
            long result = packet.GetValue();
            return result.ToString();
        }

        private static Packet ParseInput(string input)
        {
            string s = GetBinaryString(input.Trim());
            int idx = 0;
            var packet = Packet.ParsePacket(s, ref idx);
            return packet;
        }

        private static string GetBinaryString(string input)
        {
            StringBuilder sb = new StringBuilder(input.Length * 4);
            for (int i = 0; i < input.Length; i++)
            {
                sb.Append(Convert.ToString(Convert.ToInt32(input.Substring(i, 1), 16), 2).PadLeft(4, '0'));
            }
            return sb.ToString();
        }

        abstract class Packet
        {
            public readonly int Version;
            public readonly int TypeID;

            public Packet(int version, int typeID)
            {
                Version = version;
                TypeID = typeID;
            }

            public static Packet ParsePacket(string s, ref int idx)
            {
                int version = Convert.ToInt32(s.Substring(idx, 3), 2);
                idx += 3;
                int type = Convert.ToInt32(s.Substring(idx, 3), 2);
                idx += 3;

                Packet packet;
                if (type == 4)
                {
                    var dp = new DataPacket(version, type);
                    dp.Parse(s, ref idx);
                    packet = dp;
                }
                else
                {
                    var op = new OperatorPacket(version, type);
                    op.Parse(s, ref idx);
                    packet = op;
                }
                return packet;
            }

            public abstract void Parse(string s, ref int idx);

            public abstract int SumVersions();
            public abstract long GetValue();
        }

        class DataPacket : Packet
        {
            public readonly List<int> Data;

            public DataPacket(int version, int typeID) : base(version, typeID)
            {
                Data = new List<int>();
            }

            public override void Parse(string s, ref int idx)
            {
                do
                {
                    Data.Add(Convert.ToInt32(s.Substring(idx + 1, 4), 2));
                    idx += 5;
                }
                while (s[idx - 5] == '1');
            }

            public override int SumVersions()
            {
                return Version;
            }

            public override long GetValue()
            {
                long result = 0;
                foreach (var item in Data)
                {
                    result = (result << 4) + item;
                }
                return result;
            }
        }

        class OperatorPacket : Packet
        {
            public readonly List<Packet> Packets;

            public OperatorPacket(int version, int typeID) : base(version, typeID)
            {
                Packets = new List<Packet>();
            }

            public override void Parse(string s, ref int idx)
            {
                bool isPacketCnt = s[idx++] == '1';
                if (isPacketCnt)
                {
                    int len = Convert.ToInt32(s.Substring(idx, 11), 2);
                    idx += 11;
                    for (int i = 0; i < len; i++)
                    {
                        var packet = Packet.ParsePacket(s, ref idx);
                        Packets.Add(packet);
                    }
                }
                else
                {
                    int len = Convert.ToInt32(s.Substring(idx, 15), 2);
                    idx += 15;
                    int endPos = idx + len;
                    while (idx < endPos)
                    {
                        var packet = Packet.ParsePacket(s, ref idx);
                        Packets.Add(packet);
                    }
                }
            }

            public override int SumVersions()
            {
                return Packets.Sum(x => x.SumVersions()) + Version;
            }

            public override long GetValue()
            {
                long result = 0;
                var source = Packets.Select(x => x.GetValue()).ToList();
                switch (TypeID)
                {
                    case 0: result = source.Sum(); break;
                    case 1: result = Product(source); break;
                    case 2: result = source.Min(); break;
                    case 3: result = source.Max(); break;
                    case 5: result = source[0] > source[1] ? 1 : 0; break;
                    case 6: result = source[0] < source[1] ? 1 : 0; break;
                    case 7: result = source[0] == source[1] ? 1 : 0; break;
                }
                return result;
            }

            long Product(List<long> list)
            {
                long result = 1;
                foreach (var item in list)
                {
                    result *= item;
                }
                return result;
            }
        }
    }
}