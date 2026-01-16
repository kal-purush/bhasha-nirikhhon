def merge_counts(prev_doc_types, orig_counts):
    counts = {doc_type: 0 for doc_type in prev_doc_types}
    for doc_type, count in orig_counts.items():
        counts[doc_type] = count
    return counts


def counts_from_view_result(result):
    return {row['key'][0]: row['value'] for row in result['rows']}


def print_counts(counts):
    for doc_type, count in sorted(counts.items()):
        print '{}\t{}'.format(doc_type, count)


if __name__ == '__main__':
    import sys
    import json
    with open(sys.argv[1]) as f:
        prev_doc_types = f.read().strip().split('\n')
    with open(sys.argv[2]) as f:
        view_result = json.loads(f.read())

    print_counts(
        merge_counts(
            prev_doc_types,
            counts_from_view_result(view_result)
        )
    )