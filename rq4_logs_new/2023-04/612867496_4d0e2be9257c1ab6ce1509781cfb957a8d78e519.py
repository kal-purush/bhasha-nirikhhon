import csv
import ast

data = open('../artifacts/dblp.csv', 'r', encoding='utf-8')
aut = open('../artifacts/author.csv', 'a', encoding='utf-8')
pub_aut = open('../artifacts/publication-author.csv', 'a', encoding='utf-8')


def generate():
    with data, aut, pub_aut:
        reader = csv.DictReader(data)

        aut_writer = csv.writer(aut)
        pub_aut_writer = csv.writer(pub_aut)

        aut_writer.writerow(['author'])
        pub_aut_writer.writerow(['key', 'author'])

        authors = set()
        for line in reader:
            key = line['key']
            author_list = ast.literal_eval(line['author'])
            for au in author_list:
                authors.add(au)
                pub_aut_writer.writerow([key, au])
        for au in list(authors):
            aut_writer.writerow([au])


if __name__ == '__main__':
    generate()