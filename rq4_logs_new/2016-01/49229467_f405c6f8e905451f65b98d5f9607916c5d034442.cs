using System;
using MsgPack;
using MsgPack.Serialization;
using Thinknode;

namespace App
{
    /// <summary>
    /// THIS SERIALIZER IS USED FOR APPS USING DATETIME TYPES. YOU MAY REMOVE THIS TYPE
    /// IF YOUR APP IS NOT USING DATETIME TYPES.
    /// Built-in datetime serializers.
    /// </summary>
    public class DateTimeSerializer : MessagePackSerializer<DateTime>
    {
        public DateTimeSerializer(SerializationContext ownerContext) : base(ownerContext) {}

        /// <summary>
        /// Defines the serialization for the Datetime.
        /// </summary>
        /// <param name="packer">Packer.</param>
        /// <param name="objectTree">Object tree.</param>
        protected override void PackToCore(Packer packer, DateTime objectTree)
        {
            DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            double ticks = (TimeZoneInfo.ConvertTimeToUtc(objectTree) - epoch).TotalMilliseconds;
            byte[] bytes;
            try
            {
                sbyte val = Convert.ToSByte(ticks);
                bytes = BitConverter.GetBytes(val);
            }
            catch (OverflowException)
            {
                try
                {
                    short val = Convert.ToInt16(ticks);
                    bytes = BitConverter.GetBytes(val);
                }
                catch (OverflowException)
                {
                    try
                    {
                        int val = Convert.ToInt32(ticks);
                        bytes = BitConverter.GetBytes(val);
                    }
                    catch (OverflowException)
                    {
                        long val = Convert.ToInt64(ticks);
                        bytes = BitConverter.GetBytes(val);
                    }
                }
            }
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            packer.PackExtendedTypeValue(1, bytes);
        }

        /// <summary>
        /// Defines the deserialization for the Datetime.
        /// </summary>
        /// <returns>The from core.</returns>
        /// <param name="unpacker">Unpacker.</param>
        protected override DateTime UnpackFromCore(Unpacker unpacker)
        {
            DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            MessagePackObject obj = unpacker.LastReadData;
            MessagePackExtendedTypeObject ext = obj.AsMessagePackExtendedTypeObject();
            byte[] bytes = ext.GetBody();
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }
            double val;
            if (bytes.Length == 1)
            {
                val = Convert.ToDouble(bytes[0]);
            }
            else if (bytes.Length == 2)
            {
                val = Convert.ToDouble(BitConverter.ToInt16(bytes, 0));
            }
            else if (bytes.Length == 4)
            {
                val = Convert.ToDouble(BitConverter.ToInt32(bytes, 0));
            }
            else
            {
                val = Convert.ToDouble(BitConverter.ToInt64(bytes, 0));
            }
            return epoch.AddMilliseconds(val);
        }
    }

    class App : Provider
    {
        // ***
        // REPLACE THE METHODS WITHIN THESE MARKERS WITH THE METHODS DEFINED IN YOUR MANIFEST.
        // ***

        /// <summary>
        /// Add the specified integers a and b.
        /// </summary>
        /// <param name="a">The integer a.</param>
        /// <param name="b">The integer b.</param>
        public static int Add(int a, int b)
        {
            return a + b;
        }

        /// <summary>
        /// Computes the average age given the specified list of Person objects.
        /// </summary>
        /// <returns>The average age</returns>
        /// <param name="list">A list of Person objects.</param>
        public static double AverageAge(Person[] list)
        {
            double running_avg = 0;
            int count = 0;
            for (var i = 0; i < list.Length; ++i)
            {
                running_avg = ((running_avg * count) + list[i].age) / (count + 1);
                count++;
            }
            return running_avg;
        }

        /// <summary>
        /// Gets the length of the byte array.
        /// </summary>
        /// <returns>The byte array length.</returns>
        /// <param name="arr">The byte array.</param>
        public static int GetByteArrayLength(byte[] arr)
        {
            return arr.Length;
        }

        /// <summary>
        /// Gets the year component of the given datetime value.
        /// </summary>
        /// <returns>The year.</returns>
        /// <param name="val">The datetime value.</param>
        public static uint GetYear(DateTime val)
        {
            return Convert.ToUInt32(val.Year);
        }

        // ***
        // REPLACE THE METHODS WITHIN THESE MARKERS WITH THE METHODS DEFINED IN YOUR MANIFEST.
        // ***

        /// <summary>
        /// DO NOT MODIFY THIS MAIN METHOD EXCEPT ON THE LINES MARKED.
        /// The entry point of the program, where the program control starts and ends.
        /// </summary>
        /// <param name="args">The command-line arguments.</param>
        public static void Main(string[] args)
        {
            // Create App instance.
            App app = new App();

            // Add Serializers (Add your custom serializers below this line)
            app.context.Serializers.RegisterOverride(new DateTimeSerializer(app.context));

            // Start app.
            app.Start();
        }
    }
}