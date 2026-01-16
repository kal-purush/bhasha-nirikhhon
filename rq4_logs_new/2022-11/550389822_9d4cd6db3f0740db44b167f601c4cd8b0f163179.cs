using System;

namespace IntegralNumbers
{
  public static class TelemetryBuffer
  {
    /// <summary>
    /// 1: prefix byte + 8: payload bytes
    /// </summary>
    private static int _bufferSize = 9;
    private enum PayloadType
    {
      invalid,  // default
      _long, _uint, _int, _ushort, _short
    }
    private static PayloadType WhichBestType(long value)
    {
      PayloadType bestType = value switch
      {
        < int.MinValue   => PayloadType._long,
        < short.MinValue => PayloadType._int,
        < 0 => PayloadType._short,
        <= ushort.MaxValue => PayloadType._ushort,
        <= int.MaxValue    => PayloadType._int,
        <= uint.MaxValue   => PayloadType._uint,
        _ => PayloadType._long,
      };
      return bestType;
    }
    private static int PayloadSize(PayloadType type)
    {
      int size = type switch
      {
        PayloadType._long => 8,
        PayloadType._int or PayloadType._uint => 4,
        PayloadType._short or PayloadType._ushort => 2,
      };
      return size;
    }
    private static byte PrefixByte(PayloadType type)
    {
      byte b = type switch
      {
        PayloadType._ushort or PayloadType._uint => (byte)PayloadSize(type),
        _ => (byte)(Byte.MaxValue + 1 - PayloadSize(type)),
      };
      return b;
    }
    private static PayloadType PrefixType(Byte bt)
    {
      var type = bt switch
      {
        0xf8 or 0x8 => PayloadType._long,
        0xfc => PayloadType._int,
        0xfe => PayloadType._short,
        0x2 => PayloadType._ushort,
        0x4 => PayloadType._uint,
        _ => PayloadType.invalid,
      };
      return type;
    }

    public static byte[] ToBuffer(long reading)
    {
      var bytes = BitConverter.GetBytes(reading);
      var buffer = new byte[_bufferSize];
      var size = WhichBestType(reading);
      buffer[0] = PrefixByte(size);
      Buffer.BlockCopy(bytes, 0, buffer, 1, PayloadSize(size));
      return buffer;
    }

    public static long FromBuffer(byte[] buffer)
    {
      var nums = PrefixType(buffer[0]) switch
      {
        PayloadType._long   => BitConverter.ToInt64(buffer, 1),
        PayloadType._int    => BitConverter.ToInt32(buffer, 1),
        PayloadType._short  => BitConverter.ToInt16(buffer, 1),
        PayloadType._uint   => BitConverter.ToUInt32(buffer, 1),
        PayloadType._ushort => BitConverter.ToUInt16(buffer, 1),
        _ => 0,
      };
      return nums;
    }
  }
}
