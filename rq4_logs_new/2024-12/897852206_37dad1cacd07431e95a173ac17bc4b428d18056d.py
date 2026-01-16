from typing import List, Dict


def is_update_in_correct_order(update: List[str], page_ordering_rules: Dict[str, List[str]]) -> bool:
    checked_pages = []
    for page in update:
        if page in page_ordering_rules:
            required_precursor_pages = page_ordering_rules[page]
            pages_in_update_that_must_come_before_current_page = set(required_precursor_pages).intersection(update)
            if len(pages_in_update_that_must_come_before_current_page) > 0 and len(
                    set(required_precursor_pages).intersection(checked_pages)) != len(
                pages_in_update_that_must_come_before_current_page):
                return False
            checked_pages.append(page)
        else:
            checked_pages.append(page)
    return True


if __name__ == "__main__":
    with open("day_five_input.txt") as input_file:
        sections = input_file.read().split("\n\n")
        rules: List[str] = sections[0].split("\n")
        raw_updates: List[str] = sections[1].split("\n")
        page_ordering_rules: Dict[str, List[str]] = {}
        for rule in rules:
            precursor_page, required_page = rule.split("|")
            if required_page not in page_ordering_rules:
                page_ordering_rules[required_page] = [precursor_page]
            else:
                page_ordering_rules[required_page].append(precursor_page)
        updates: List[List[str]] = []
        for raw_update in raw_updates:
            updates.append(raw_update.split(","))

        updates_in_correct_order = []
        for update in updates:
            if is_update_in_correct_order(update, page_ordering_rules):
                updates_in_correct_order.append(update)

        result = 0
        for update_in_correct_order in updates_in_correct_order:
            num_of_pages = len(update_in_correct_order)
            middle_point = int(num_of_pages / 2)
            middle_value = int(update_in_correct_order[middle_point])
            result += middle_value
        print(result)