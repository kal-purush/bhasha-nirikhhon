import csv
"""
Name: Gali
ID:322060187
Group:01
Assignment:ass7
"""

HYPERLINK_MAGIC = '<a href="'


def parse_single_html(html_content):
    """
    * Function Name:parse_single_html
    * Input:html_content: The contents of the html file.
    * Output: List of files names referenced in the html.
    * Function Operation: Parses a single html file and finds the references files in it.
    """
    files = []
    starting_index = html_content.find(HYPERLINK_MAGIC)
    while starting_index != -1:
        ending_index = html_content.find('"', starting_index + len(HYPERLINK_MAGIC))
        files.append(html_content[starting_index + len(HYPERLINK_MAGIC):ending_index])
        html_content = html_content[ending_index:]
        starting_index = html_content.find(HYPERLINK_MAGIC)

    return files


def crawl(root_html, results):
    """
    *Function Name:crawl
    *Input:root_html,results
    *Output: return list of opened files
    *Function Operation:open html files and save them
    """
    with open(root_html, "r") as root_html_file:
        html_content = root_html_file.read()

    if root_html in results:
        return

    files = parse_single_html(html_content)
    if not files:
        results[root_html] = []
        return

    results[root_html] = files
    for f in files:
        crawl(f, results)


def save_results_to_csv(csv_name, results_dict):
    """
    *Function Name:save_results_to_csv
    *Input:csv_name,results_dict
    *Output: csv file
    *Function Operation:create csv and write our information there
    """
    rows = []
    for key in results_dict:
        rows.append([key] + results_dict[key])

    # Writes the csv
    with open(csv_name, "w", newline="") as results_csv:
        writer = csv.writer(results_csv)
        writer.writerows(rows)


def print_sorted_links(file_name, results_dict):
    """
   *Function Name:print_sorted_links
    *Input:file_name,results_dict
    *Output:print sorted file
    *Function Operation: print if there no html file and print sorted file
    """
    if file_name not in results_dict:
        print("{} is not referenced".format(file_name))

    print(sorted(results_dict[file_name]))


def main():
    results = {}
    root_file_name = input("enter source file:\n")
    crawl(root_file_name, results)
    save_results_to_csv("results.csv", results)
    root_file_name = input("enter file name:\n")
    print_sorted_links(root_file_name, results)


if __name__ == "__main__":
    main()