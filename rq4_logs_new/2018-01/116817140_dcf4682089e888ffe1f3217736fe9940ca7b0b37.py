from collections import defaultdict

with open('day4.txt', 'r') as f:
    sum_sector_ids = 0

    for line in f:
        split_line = line.strip()[:-1].split('-')
        letters = [letter for each_group in split_line[:-1] for letter in each_group]
        if len(letters) < 5:
            continue
        sector_checksum_split = split_line[len(split_line) - 1].split('[')
        sector = int(sector_checksum_split[0])
        checksum = sector_checksum_split[1]

        letter_dict = defaultdict(int)
        for letter in letters:
            letter_dict[letter] += 1
        letter_count = defaultdict(list)
        for k, v in letter_dict.items():
            letter_count[v].append(k)
        sorted_letter_count = sorted(letter_count.items(), reverse=True)
        all_sorted_letter_count = [(each[0], sorted(each[1])) for each in sorted_letter_count]
        expected_checksum = ''
        for each in all_sorted_letter_count:
            if len(expected_checksum) == 5:
                break
            for letter in each[1]:
                if len(expected_checksum) == 5:
                    break
                expected_checksum += letter
        if expected_checksum == checksum:
            last_dash = line.strip().rfind('-')
            encrypted = line[:last_dash]
            unencrypted = ''
            for letter in encrypted:
                unencrypted += ' ' if letter == '-' else chr(((ord(letter) - 97 + sector) % 26) + 97)
            if unencrypted.find('north') != -1:
                print(sector)



