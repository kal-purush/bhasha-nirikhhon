def is_abba(input_str: str) -> bool:
    first = input_str[0:2]
    second = input_str[3] + input_str[2]

    return first == second and input_str[0] != input_str[1]


def is_aba(input_str: str) -> bool:
    return input_str[0] == input_str[2] and input_str[0] != input_str[1]


def create_target(input_str: str, start_index: int, min_length: int) -> str:
    if len(input_str) - start_index < min_length:
        return ''

    return input_str[start_index:start_index + min_length]


def contains_abba(potential_abbas: list) -> bool:
    for each in potential_abbas:
        for i in range(len(each)):
            potential_abba = create_target(each, i, 4)
            if potential_abba != '' and is_abba(potential_abba):
                return True

    return False


def get_list_abas(potential_abas: list) -> list:
    abas = []
    for each in potential_abas:
        for i in range(len(each)):
            potential_aba = create_target(each, i, 3)
            if potential_aba != '' and is_aba(potential_aba):
                abas.append(potential_aba)

    return abas

with open('day7.txt', 'r') as f:
    begin_hs = '['
    end_hs = ']'
    tls_count = 0
    ssl_count = 0

    for line in f:
        line = line.strip()
        current_index = 0
        hss = []
        non_hss = []
        is_end = False
        while not is_end:
            start_hs = line.find(begin_hs, current_index)
            if start_hs == -1:
                is_end = True
            start_hs = start_hs if start_hs != -1 else len(line)
            non_hss.append(line[current_index:start_hs])

            if is_end:
                break

            end_hs_index = line.find(end_hs, start_hs)
            hss.append(line[start_hs + 1:end_hs_index])

            current_index = end_hs_index + 1

        if contains_abba(non_hss):
            if not contains_abba(hss):
                tls_count += 1

        abas = get_list_abas(non_hss)
        found_match = False
        for each_aba in abas:
            if not found_match:
                for each_hs in hss:
                    if each_hs.find(each_aba[1] + each_aba[0] + each_aba[1]) != -1:
                        ssl_count += 1
                        found_match = True
                        break
            else:
                break

    print(tls_count)
    print(ssl_count)

