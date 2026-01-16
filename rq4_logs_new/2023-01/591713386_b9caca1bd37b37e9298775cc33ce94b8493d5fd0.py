import csv
import mysql.connector
import json
import re
import ast
import sys
import random
import string

set_id = set()


def remove_duplicate(file1, file2, index):
    with open(file1, 'r') as input_file, open(file2, 'w') as output_file:
        reader = csv.reader(input_file)
        writer = csv.writer(output_file)

        seen = set()

        for row in reader:
            row = tuple(row)
            if row[index] != "id":
                if int(row[index]) not in seen:
                    writer.writerow(row)
                    seen.add(int(row[index]))


def credit(mydbb):
    remove_duplicate('credits.csv', 'credits2.csv', 2)
    with open('credits2.csv', 'r', encoding='utf-8') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    j = 0
    t = 0
    mycursor = mydbb.cursor()
    actor_list = []
    caracter_list = []
    director_list = []
    id_list = []
    z = 0
    for li in list_of_csv:
        if int(li[2]) in set_id:
            z = z + 1
            if z % 1000 == 0:
                print(z)
            if li[0] == 'cast':
                continue
            try:
                cast = ast.literal_eval(li[0])
                crew = ast.literal_eval(li[1])
            except:
                t = t + 1
                continue
            if li[0] != 'cast':
                cast = []
                try:
                    cast = ast.literal_eval(li[0])
                except:
                    print(t)
                    t = t + 1
                    continue
                caracter_subList = []
                actor_subList = []
                for x in cast:
                    if 'character' not in x:
                        continue
                    if 'name' not in x:
                        continue
                    sql1 = "INSERT INTO actors (actor_name,movie_id, caracter) VALUES (%s, %s, %s)"
                    val1 = (x['name'], li[2], x['character'])
                    mycursor.execute(sql1, val1)
                    mydbb.commit()
                    caracter_subList.append(x['character'])
                    actor_subList.append(x['name'])

            if li[1] != 'crew':
                crew = []
                try:
                    crew = ast.literal_eval(li[1])
                except:
                    print(t)
                    t = t + 1

                    continue
                for x in crew:
                    if x['job'] == 'Director':
                        sql2 = "INSERT INTO directors (director,movie_id) VALUES (%s, %s)"
                        val2 = (x['name'], li[2])
                        mycursor.execute(sql2, val2)
                        mydbb.commit()
                        director_list.append(x['name'])

            if li[2] != 'id':
                id_list.append(li[2])
                j += 1


def movie(mydbb):
    # remove_duplicate('movies_metadata.csv', 'movies_metadata2.csv', 5)
    with open('movies_metadata2.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    j = 0
    # 0
    adult_list = []
    # 2
    budget_list = []
    # 3
    genre_list = []
    # 5
    id_list = []
    # 7
    language_list = []
    # 8
    title_list = []
    # 9
    overview_list = []
    # 10
    popularity_list = []
    # 15
    revenue_list = []
    # 22
    vote_list = []
    for li in list_of_csv:
        genres = {}
        try:
            genres = ast.literal_eval(li[3])
        except:
            continue
        if li[0] != 'adult':
            adult_list.append(li[0])
        if li[2] != 'budget':
            budget_list.append(int(li[2]))
        if li[3] != 'genres':
            for x in genres:
                sql1 = "INSERT INTO genres (genre,movie_id) VALUES (%s, %s)"
                val1 = (x["name"], li[5])
                mycursor.execute(sql1, val1)
                mydbb.commit()
        if li[5] != 'id':
            id_list.append(int(li[5]))
            set_id.add(int(li[5]))
        if li[7] != 'original_language':
            language_list.append(li[7])
        if li[8] != 'original_title':
            title_list.append(li[8])
        if li[9] != 'overview':
            overview_list.append(li[9])
        if len(li) > 10:
            if li[10] != 'popularity':
                popularity_list.append(float(li[10]))
        else:
            popularity_list.append(0)
        if len(li) > 15:
            if li[15] != 'revenue':
                revenue_list.append(float(li[15]))
        else:
            revenue_list.append(0)
        if len(li) > 22:
            if li[22] != 'vote_average':
                vote_list.append(float(li[22]))
        else:
            vote_list.append(0)

    for i in range(0, len(id_list)):
        sql1 = "INSERT INTO movies_data (movie_id,budget,adult,title,lang,overview,popularity,revenue,vote_average) VALUES (%s,%s, %s, %s, %s,%s, %s,%s,%s)"
        val1 = (id_list[i], budget_list[i], adult_list[i], title_list[i], language_list[i], overview_list[i],
                popularity_list[i], revenue_list[i], vote_list[i])
        mycursor.execute(sql1, val1)
        mydbb.commit()


def keywordd(mydbb):
    # remove_duplicate('keywords.csv', 'keywords2.csv', 0)
    with open('keywords2.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    id_list = []
    key_list = []
    for li in list_of_csv:
        if int(li[0]) in set_id:
            if li[0] != 'id':
                id_list.append(int(li[0]))
            name = li[1].split(",")
            strr = ""
            for i in range(0, len(name)):
                if i % 2 != 0:
                    first = name[i].split("'name': ")
                    second = first[1].split("}")
                    strr += second[0]
                    strr += ", "
            if strr != "":
                key_list.append(strr)
            else:
                if li[0] != 'id':
                    key_list.append("None")

    for i in range(0, len(id_list)):
        sql = "INSERT INTO keyword (keyword, movie_id) VALUES (%s, %s)"
        val = (key_list[i], id_list[i])
        mycursor.execute(sql, val)
        mydbb.commit()


def get_random_pass(length):
    result_str = ''.join(random.choice(string.ascii_letters) for i in range(length))
    return result_str


def user(mydbb):
    with open('NationalNames.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    user_id = []
    user_name = []
    user_password = []
    i = 1
    j = 7
    for li in list_of_csv:
        passw = ""
        if li[1] != 'Name':
            user_name.append(li[1])
            user_id.append(i)
            passw = get_random_pass(j)
            user_password.append(passw)
            i += 1
            j += 1
        if i == 5178:
            break
        if j == 16:
            j = 7

    for i in range(0, len(user_id)):
        sql = "INSERT INTO users (user_id, name, password) VALUES (%s, %s, %s)"
        val = (user_id[i], user_name[i], user_password[i])
        mycursor.execute(sql, val)
        mydbb.commit()


def dbb(hostt, userr, passwordd, databasee):
    mydbb = mysql.connector.connect(
        host=hostt,
        user=userr,
        password=passwordd,
        database=databasee
    )
    mycursor = mydbb.cursor()
    mycursor.execute("CREATE TABLE actors (actor_name LONGTEXT,movie_id int ,caracter LONGTEXT)")
    mycursor.execute("CREATE TABLE directors (director LONGTEXT,movie_id int)")
    mycursor.execute("CREATE TABLE genres (genre LONGTEXT,movie_id int)")
    mycursor.execute(
        "CREATE TABLE movies_data (movie_id int ,budget int,adult LONGTEXT,title LONGTEXT,lang VARCHAR(45),overview LONGTEXT, popularity float,revenue float, vote_average FLOAT)")
    mycursor.execute("CREATE TABLE users (user_id int,name LONGTEXT,password LONGTEXT)")
    mycursor.execute("CREATE TABLE recommendation (user_id1 int, user_id2 int,movie_id int)")
    mycursor.execute("CREATE TABLE keyword (keyword LONGTEXT,movie_id int)")
    return mydbb


def relation(mydbb, name):
    mycursor = mydbb.cursor()
    mycursor.execute("USE " + name + ";")
    mycursor.execute("ALTER TABLE movies_data ADD PRIMARY KEY (movie_id);")
    mycursor.execute("ALTER TABLE genres ADD FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id);")
    mycursor.execute("ALTER TABLE keyword ADD FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id);")
    mycursor.execute("ALTER TABLE actors ADD FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id);")
    mycursor.execute("ALTER TABLE directors ADD FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id);")
    mycursor.execute("ALTER TABLE recommendation ADD FOREIGN KEY (movie_id) REFERENCES movies_data(movie_id);")


if __name__ == "__main__":
    mydb = dbb(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    movie(mydb)
    user(mydb)
#    keywordd(mydb)
    credit(mydb)
    relation(mydb, sys.argv[4])