class ArrayHelper:
    def divide(self, array, parts):
        if array is None:
            raise ValueError("Invalid value for 'array' – '{}'".format(array))
        if parts is None or parts <= 0:
            raise ValueError("Invalid value for 'parts' – '{}'".format(parts))

        part_size = self.__get_part_size(len(array), parts)
        divisions = self.__get_divisions(array, parts, part_size)
        remainder = self.__get_remainder(array, parts, part_size)
        divisions[-1].extend(remainder)

        return divisions

    def __get_part_size(self, array_length, parts):
        part_size = array_length // parts

        if array_length % parts == parts - 1:
            part_size += 1

        return part_size

    def __get_divisions(self, array, parts, part_size):
        return [array[(i * part_size):(i * part_size) + part_size] for i in range(0, parts)]

    def __get_remainder(self, array, parts, part_size):
        return array[(parts * part_size):]