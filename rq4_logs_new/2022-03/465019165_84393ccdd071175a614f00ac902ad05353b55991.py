import argparse
import random




def split_files(input_text_file, input_labels_file, dev_indexes, test_indexes, train_indexes, prefix):
    dev_file_text = open(f"{prefix + input_text_file }.dev.text", "w", encoding="utf-8")
    test_file_text = open(f"{prefix + input_text_file }.test.text", "w", encoding="utf-8")
    train_file_text = open(f"{prefix + input_text_file }.train.text", "w", encoding="utf-8")
    dev_file_labels = open(f"{prefix + input_text_file }.dev.labels", "w", encoding="utf-8")
    test_file_labels = open(f"{prefix + input_text_file }.test.labels", "w", encoding="utf-8")
    train_file_labels = open(f"{prefix + input_text_file }.train.labels", "w", encoding="utf-8")
    dev_indexes = dict.fromkeys(dev_indexes)
    test_indexes = dict.fromkeys(test_indexes)
    train_indexes = dict.fromkeys(train_indexes)
    total_lines_written = 0
    with open(input_text_file, encoding="utf-8") as txt_stream:
        for text_index, text_line in enumerate(txt_stream.readlines()):
            if text_index in dev_indexes:
                dev_file_text.write(text_line)
                total_lines_written += 1
                continue
            if text_index in test_indexes:
                test_file_text.write(text_line)
                total_lines_written += 1
                continue
            if text_index in train_indexes:
                train_file_text.write(text_line)
                total_lines_written += 1
                continue
    with open(input_labels_file, encoding="utf-8") as stream:
        for index, label_line in enumerate(stream.readlines()):
            if index in dev_indexes:
                dev_file_labels.write(label_line)
                total_lines_written += 1
                continue
            if index in test_indexes:
                test_file_labels.write(label_line)
                total_lines_written += 1
                continue
            if index in train_indexes:
                train_file_labels.write(label_line)
                total_lines_written += 1
                continue
    dev_file_text.close()
    test_file_text.close()
    train_file_text.close()
    dev_file_labels.close()
    test_file_labels.close()
    train_file_labels.close()
    return total_lines_written

def split(
        input_text_file,
        input_labels_file,
        dev_percent,
        train_percent,
        test_percent,
        prefix,
        seed,
):
    # seed random for reproducibility
    random.seed(seed)
    # perform wc -l
    line_count_labels = count_lines(input_labels_file)
    line_count_text = count_lines(input_text_file)
    print(line_count_text, line_count_labels)
    assert line_count_text == line_count_labels, "IOB labels file line count doesnt match text file count"
    lines = list(range(0, line_count_labels))
    assert dev_percent + train_percent + test_percent == 100
    # shuffle the list
    random.shuffle(lines)
    range_dev = int(round(len(lines) * dev_percent * 0.01, 5))
    range_train = int(round(len(lines) * train_percent * 0.01, 5))
    range_test = int(round(len(lines) * test_percent * 0.01, 5))
    diff = len(lines) - (range_train + range_dev + range_test)
    range_test += diff
    assert range_train + range_dev + range_test == len(lines)
    dev_indexes = lines[0: range_dev]
    test_indexes = lines[range_dev: (range_dev + range_test)]
    train_indexes = lines[(range_dev + range_test):]

    split_files(input_text_file, input_labels_file, dev_indexes, test_indexes, train_indexes, "")



def blocks(file_stream, size=65536):
    while True:
        b = file_stream.read(size)
        if not b: break
        yield b



def count_lines(file_name):
    lines = 0
    with open(file_name, "r", encoding="utf-8") as f:
        lines += sum(bl.count("\n") for bl in blocks(f))
    return lines

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Split files')
    parser.add_argument('-s', '--seed', help='randomness seed')
    parser.add_argument('-t', '--text-file', help='Text file')
    parser.add_argument('-l', '--labels-file', help='labels file')
    parser.add_argument('-p', '--output-prefix', help="output prefix")
    parser.add_argument('-d', '--dev-size', help="dev size percentage")
    parser.add_argument('-tr', '--train-size', help="train size percentage")
    parser.add_argument('-ts', '--test-size', help="train size percentage")
    args = parser.parse_args()

    split(
        input_text_file=args.text_file,
        input_labels_file=args.labels_file,
        dev_percent=int(args.dev_size),
        train_percent=int(args.train_size),
        test_percent=int(args.test_size),
        prefix=args.output_prefix,
        seed=args.seed
    )
