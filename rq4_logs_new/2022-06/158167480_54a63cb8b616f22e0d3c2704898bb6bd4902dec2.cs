using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using GeoLibrary.Extension;
using GeoLibrary.Model;

namespace GeoLibrary.IO.Wkt
{
    public static class WktReader
    {
        public static Geometry Read(string wkt)
        {
            var reader = wkt.AsSpan();
            var type = ReadType(ref reader);

            SkipWhiteSpaces(ref reader);

            if (type.Equals(WktTypes.Point, StringComparison.OrdinalIgnoreCase))
            {
                return ReadPoint(ref reader);
            }
            else if (type.Equals(WktTypes.MultiPoint, StringComparison.OrdinalIgnoreCase))
            {
                return ReadMultiPoint(ref reader);
            }
            else if (type.Equals(WktTypes.LineString, StringComparison.OrdinalIgnoreCase))
            {
                return ReadLineString(ref reader);
            }
            else if (type.Equals(WktTypes.Polygon, StringComparison.OrdinalIgnoreCase))
            {
                return ReadPolygon(ref reader);
            }
            else if (type.Equals(WktTypes.MultiPolygon, StringComparison.OrdinalIgnoreCase))
            {
                return ReadMultiPolygon(ref reader);
            }
            else
            {
                throw new ArgumentException($"Not supported WKT type: {type.ToString()}");
            }
        }

        private static ReadOnlySpan<char> ReadType(ref ReadOnlySpan<char> reader)
        {
            int i = 0;
            for (; i < reader.Length; i++)
            {
                if (!IsLetter(reader[i]))
                    break;
            }

            var type = reader.Slice(0, i);
            reader = reader.Slice(i);

            return type;
        }

        private static Point ReadPoint(ref ReadOnlySpan<char> reader)
        {
            VerifyChar(ref reader, '(');

            return ReadPointInner(ref reader);
        }

        private static MultiPoint ReadMultiPoint(ref ReadOnlySpan<char> reader)
        {
            var points = new List<Point>();
            VerifyChar(ref reader, '(');

            var isOneArray = reader.IsEmpty == false && reader[0] != '(';
            while (reader.IsEmpty == false)
            {
                points.Add(isOneArray ? ReadPointInner(ref reader) : ReadPoint(ref reader));
                SkipWhiteSpaces(ref reader);
                if (isOneArray == false)
                    VerifyChar(ref reader, ')');

                if (reader.IsEmpty == false && reader[0] == ')')
                    break;

                VerifyChar(ref reader, ',');
                SkipWhiteSpaces(ref reader);
            }

            return new MultiPoint(points);
        }

        private static LineString ReadLineString(ref ReadOnlySpan<char> reader)
        {
            return new LineString(ReadPointsInner(ref reader));
        }

        private static Polygon ReadPolygon(ref ReadOnlySpan<char> reader)
        {
            var lineStrings = new List<LineString>();
            VerifyChar(ref reader, '(');

            while (reader.IsEmpty == false)
            {
                lineStrings.Add(new LineString(ReadPointsInner(ref reader)));
                SkipWhiteSpaces(ref reader);
                if (reader.IsEmpty == false && reader[0] == ')')
                    break;

                VerifyChar(ref reader, ',');
                SkipWhiteSpaces(ref reader);
            }

            return new Polygon(lineStrings);
        }

        private static MultiPolygon ReadMultiPolygon(ref ReadOnlySpan<char> reader)
        {
            var polygons = new List<Polygon>();
            VerifyChar(ref reader, '(');

            while (reader.IsEmpty == false)
            {
                polygons.Add(ReadPolygon(ref reader));
                SkipWhiteSpaces(ref reader);
                VerifyChar(ref reader, ')');
                if (reader.IsEmpty == false && reader[0] == ')')
                    break;

                VerifyChar(ref reader, ',');
                SkipWhiteSpaces(ref reader);
            }

            return new MultiPolygon(polygons);
        }

        private static Point ReadPointInner(ref ReadOnlySpan<char> reader)
        {
            return new Point(ReadDouble(ref reader), ReadDouble(ref reader));
        }

        private static IEnumerable<Point> ReadPointsInner(ref ReadOnlySpan<char> reader)
        {
            var points = new List<Point>();
            VerifyChar(ref reader, '(');

            while (reader.IsEmpty == false)
            {
                points.Add(ReadPointInner(ref reader));
                SkipWhiteSpaces(ref reader);

                if (reader.IsEmpty == false && reader[0] == ')')
                {
                    reader = reader.Slice(1);
                    break;
                }

                VerifyChar(ref reader, ',');
                SkipWhiteSpaces(ref reader);
            }

            return points;
        }

        private static double ReadDouble(ref ReadOnlySpan<char> reader)
        {
            // skip whitespaces in head
            SkipWhiteSpaces(ref reader);

            int length = 0;
            var style = NumberStyles.None;
            while (true)
            {
                var ch = reader[length];

                if (IsBetween(ch, '0', '9'))
                {
                }
                else if (ch == '.')
                {
                    style |= NumberStyles.AllowDecimalPoint;
                }
                else if (ch == '-')
                {
                    style |= NumberStyles.AllowLeadingSign;
                }
                else if (ch == 'e' || ch == 'E')
                {
                    style |= NumberStyles.AllowExponent;
                }
                else
                {
                    break;
                }

                length++;
            }

            if (length == 0)
            {
                throw new ArgumentException("Invalid number");
            }

            var result = double.Parse(reader.Slice(0, length), style, CultureInfo.InvariantCulture);
            reader = reader.Slice(length);

            return result;
        }

        private static void SkipWhiteSpaces(ref ReadOnlySpan<char> reader)
        {
            int i = 0;
            for (; i < reader.Length; i++)
            {
                if (reader[i] != ' ')
                {
                    break;
                }
            }

            reader = reader.Slice(i);
        }

        private static bool IsBetween(char c, char minInclusive, char maxInclusive) =>
            (uint)(c - minInclusive) <= (uint)(maxInclusive - minInclusive);

        private static bool IsLetter(char ch)
        {
            return (uint)((ch | 0x20) - 'a') <= 'z' - 'a';
        }

        private static void VerifyChar(ref ReadOnlySpan<char> reader, char @char)
        {
            if (reader.IsEmpty || reader[0] != @char)
                throw new ArgumentException($"Invalid WKT! Expect '{@char}' but '{(reader.IsEmpty ? -1 : reader[0])}'");

            reader = reader.Slice(1);
        }
    }
}