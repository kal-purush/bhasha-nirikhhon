import argparse


class HexCodec(object):

    HEX_DIGITS = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                  'a', 'b', 'c', 'd', 'e', 'f')

    def lower_hex_to_unsigned_long(self, lower_hex):
        """
        Parses a 1 to 32 character lower-hex string with no prefix into an unsigned long,
        tossing any bits higher than 64.
        """
        length = len(lower_hex)
        if length < 1 or length > 32:
            raise Exception("Input not the right length")

        begin_index = length - 16 if length > 16 else 0

        return self.convert_to_long(lower_hex, begin_index)

    def convert_to_long(self, lower_hex, index):
        result = 0
        end_index = min(index + 16, len(lower_hex))
        for char in lower_hex[index:end_index]:
            result <<= 4
            try:
                result |= self.HEX_DIGITS.index(char)
            except ValueError:
                raise Exception("Invalid hex character found")
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Test for trace sampling")
    parser.add_argument('trace_id', type=str,
                        help='Trace ID to test')
    parser.add_argument('sample_rate', type=float,
                        help='Rate at which trace sampling occurs')
    args = parser.parse_args()
    codec = HexCodec()
    # Constant is the Java's long.MAX_VALUE
    boundary = 9223372036854775807 * args.sample_rate
    trace_id = codec.lower_hex_to_unsigned_long(args.trace_id)
    print trace_id <= boundary