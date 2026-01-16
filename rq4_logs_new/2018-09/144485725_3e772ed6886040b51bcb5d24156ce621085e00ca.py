import sqlite3
import calculations
from save_data import read_old_courses


class Transcript:
    def __init__(self):
        self.user_name = ""
        self.path = ""

    def set_user_name(self, user_name):
        if self.user_name == "":
            self.user_name = user_name
            return True
        else:
            return False

    def set_path(self, path):
        if self.path == "":
            self.path = path
            return True
        else:
            return False

    def get_transcript_data(self):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        seasons = cursor.execute("SELECT * FROM seasons").fetchall()
        seasons = self.sort_season(seasons)

        transcript_data = []
        for row in seasons:
            courses = cursor.execute("SELECT * FROM courses WHERE Season_Name == '" + row[0] + "'")

            transcript_data.append([row[0][:9], row[0][12:], row[3], round(row[2], 2), row[1], "Season", row[0], 0])
            for course in courses:
                if course[2]:
                    course_code = course[1] + "E"
                else:
                    course_code = course[1]

                transcript_data.append([course_code, course[3], course[4], course[5], row[1], "Course", row[0],
                                        course[7]])
        connection.close()

        return transcript_data

    def get_editable_data(self):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        seasons = cursor.execute("SELECT * FROM seasons").fetchall()
        seasons = self.sort_season(seasons)

        editable_seasons = []
        for row in seasons:
            if row[1]:
                editable_seasons.append(row[0])

        courses = cursor.execute("SELECT * FROM courses").fetchall()

        editable_courses = []
        for course in courses:
            editable_courses.append([course[1], course[5]])
        connection.close()

        return editable_courses

    def get_last_season(self):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        seasons = cursor.execute("SELECT * FROM seasons").fetchall()
        seasons = self.sort_season(seasons)

        last_season = ""
        for season in reversed(seasons):
            if not season[1]:
                last_season = season[0]
                break

        connection.close()

        return last_season

    def get_gpa(self):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        data = cursor.execute("SELECT Credit, Grade_Numeric FROM courses WHERE Passive == 0")
        gpa = calculations.calculate_gpa(data)

        connection.close()

        return gpa

    def get_season_gpa(self, season_name):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        gpa = cursor.execute("SELECT GPA FROM seasons WHERE Season_Name == '" + season_name + "'").fetchone()

        connection.close()

        return round(gpa[0], 2)

    def get_course(self, course_code):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        course = cursor.execute("SELECT * FROM seasons WHERE Course_Code == '" + course_code + "'").fetchall()

        connection.close()

        return course

    def update_course(self, season_name, course_code, new_grade, passive=None):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        if passive is None:
            cursor.execute("UPDATE courses SET Grade = '" + new_grade + " ', Grade_Numeric = " +
                           str(calculations.solve_grade(new_grade)) + " WHERE Season_Name == '" + season_name +
                           "' AND Course_Code == '" + course_code + "'")
        else:
            cursor.execute("UPDATE courses SET Grade = '" + new_grade + " ', Passive = " + passive +
                           " WHERE Season_Name == '" + season_name + "' AND Course_Code == '" + course_code + "'")
        connection.commit()
        connection.close()

    def update_season_gpa(self, season_name):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        data = cursor.execute("SELECT Credit, Grade_Numeric FROM courses WHERE Season_Name == '" + season_name + "'")
        cursor.execute("UPDATE seasons SET GPA = " + str(calculations.calculate_gpa(data)) +
                       " WHERE Season_Name == '" + season_name + "'")

        connection.commit()
        connection.close()

    def update_season_credit(self, season_name):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        credit = cursor.execute("SELECT Credit FROM courses WHERE Season_Name == '" + season_name + "'").fetchall()
        total_credit = 0.0
        for data in credit:
            total_credit += data[0]

        cursor.execute("UPDATE seasons SET Total_Credit = " + str(total_credit) +
                       " WHERE Season_Name == '" + season_name + "'")

        connection.commit()
        connection.close()

    def add_season(self, season_name):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("INSERT INTO seasons (Season_Name, Editable) VALUES (?,?)", [season_name, 1])
        connection.commit()

        connection.close()

    def add_course(self, course_data):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()
        if course_data[0][-1] == "E":
            course_code = course_data[0][:-1]
            english_course = 1
        else:
            course_code = course_data[0]
            english_course = 0

        cursor.execute("INSERT INTO courses VALUES (?,?,?,?,?,?,?,?)", [course_data[5], course_code, english_course,
                                                                        course_data[1], course_data[2], course_data[3],
                                                                        calculations.solve_grade(course_data[3]),
                                                                        course_data[4]])
        connection.commit()

        courses = cursor.execute("SELECT Season_Name, Course_Code, Passive FROM courses").fetchall()

        for course in courses:
            if course[1] == course_code and course[0] != course_data[5]:
                cursor.execute("UPDATE courses SET Passive = 1 WHERE Season_Name == '" + course[0] +
                               "' AND Course_Code == '" + course_code + "'")

        connection.close()

    def check_course_before_added(self, course_code):
        if course_code[-1] == "E":
            course_code = course_code[:-1]

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        course = cursor.execute("SELECT * FROM courses WHERE Course_Code == '" + course_code + "'").fetchall()
        connection.close()

        if course:
            if course[-1][7] == 0:
                self.update_course(course[-1][0], course[-1][1], course[-1][5] + "*", '1')
                return
            else:
                self.update_course(course[-1][0], course[-1][1], course[-1][5][:-2], '0')
                return

        old_courses = read_old_courses()

        if course_code in old_courses:
            self.check_course_before_added(old_courses[course_code])
            return

    def delete_course(self, season_name, course_code, check=True):
        if course_code[-1] == "E":
            course_code = course_code[:-1]

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("DELETE FROM courses WHERE Season_Name == '" + season_name + "' AND Course_Code == '"
                       + course_code + "'")
        connection.commit()
        connection.close()

        if check is True:
            self.check_course_before_added(course_code)

    def delete_season(self, season_name):
        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        courses = cursor.execute("SELECT * FROM courses WHERE  Season_Name == '" + season_name + "'").fetchall()
        cursor.execute("DELETE FROM seasons WHERE Season_Name == '" + season_name + "'")
        connection.commit()
        connection.close()
        for course in courses:
            if not course[7]:
                self.delete_course(season_name, course[1])

    @staticmethod
    def sort_season(seasons):
        for i in reversed(range(len(seasons))):
            for j in range(i):
                if seasons[j][0][:9] > seasons[j+1][0][:9]:
                    seasons[j], seasons[j+1] = seasons[j+1], seasons[j]
                elif seasons[j][0][:9] == seasons[j+1][0][:9]:
                    if seasons[j][0][12:] == "Yaz Öğretimi":
                        seasons[j], seasons[j + 1] = seasons[j + 1], seasons[j]
                    elif seasons[j][0][12:] == "Bahar" and seasons[j+1][0][12:] == "Güz":
                        seasons[j], seasons[j + 1] = seasons[j + 1], seasons[j]
        return seasons


if __name__ == "__main__":
    tr = Transcript()
    tr.set_user_name("kadio")
    tr.set_path("Data/Transcripts/kadio.dp")
    tr_data = tr.get_transcript_data
    print(tr.get_last_season())
    print(tr.get_gpa())