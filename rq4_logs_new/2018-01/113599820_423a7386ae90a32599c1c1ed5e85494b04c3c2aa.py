from Utils import Utils

"""
这个脚本将高校招生专业要求0905.csv的数据，转化生成为Insert SQL
CSV文件格式：
第一列：大学专业或者门类
第二列：高中课程
第三列：大学名称

将分为6步：

1. 去除第二列高中课程为不限的数据
2. 把带「类」字的二级科目展开到三级一行，xx类 → xx1|xx2
3. 将第一列三级专业科目xx1|xx2展开到多条数据
4. 将第三列高中课程 地理|历史 展开到多行数据
5. 将经过3，4步展开后的数据按大学、专业、高中课程，是否相同的逻辑来去重
6. 更新本科大学-分成将大学专业、高中课程、大学名称，转换到对应表的ID，并生成SQL
7. 更新专科大学
"""


def expand_2_to_3(input_path):
    """
    将二级类数据展开到三级
    :return:
    """
    read_data = Utils.read_csv(input_path)

    invalid_data, result_data, dict_data = [], [], {}
    db_data_2 = Utils.get_data("SELECT \
                                  a.major_name, \
                                  a.major_code, \
                                  group_concat(b.major_name SEPARATOR '|') \
                                FROM university_major_list a LEFT JOIN university_major_list b ON a.id = b.p_id AND b.type = 3 \
                                WHERE a.type = 2 AND a.major_code < '1399' \
                                GROUP BY a.major_name,a.major_code \
                                ORDER BY a.major_code")
    for row in db_data_2:
        dict_data[row[0]] = row[2]
    # 区分含有'不限'的数据
    for row in read_data:
        row = row.strip()
        if row.find("不限") >= 0:
            invalid_data.append(row)
        else:
            tmp = row.split(",")
            if tmp[0].find("类") >= 0 and dict_data.get(tmp[0]) is not None:
                tmp[0] = dict_data.get(tmp[0])
            result_data.append(",".join(tmp))

    return invalid_data, result_data


def expand_str_to_list(input_path):
    tmp_list, result_data = [], []

    data = Utils.read_csv(input_path)
    # 展开第一列
    for row in data:
        tmp = row.strip().split(",")
        if tmp[0].find("|") >= 0:
            col_1 = tmp[0].split("|")
            for c in col_1:
                tmp_list.append(c + "," + tmp[1] + "," + tmp[2])
    # 展开第二列
    for row in tmp_list:
        tmp = row.split(",")
        if tmp[1].find("|") >= 0:
            col_1 = tmp[1].split("|")
            for c in col_1:
                result_data.append(tmp[0] + "," + c + "," + tmp[2])

    return list(set(result_data))
    # return result_data


def convert_data_to_sql(file, education_level=1):
    correct_data, wrong_data = [], []
    # 大学专业列表
    sql = 'SELECT concat(a.university_name, b.major_name) as `key`, c.id FROM university_list a, university_major_list b, ' \
          'university_has_major c WHERE a.id = c.university_list_id AND b.id = c.university_major_list_id'
    if education_level == 1:
        sql += ' AND a.education_level LIKE "%本科%"'
    else:
        sql += ' AND a.education_level LIKE "%高职(专科)%"'

    data_dict = dict(Utils.get_data(sql))

    # 高校列表
    high_courses = {"物理": "1", "化学": "2", "生物": "3",
                    "思想政治": "4", "历史": "5", "地理": "6", "技术": "7"}

    data_list = Utils.read_csv(file)
    sql = "INSERT INTO `university_has_major_has_school_course` (`university_has_major_id`, `high_school_course_id`) VALUES "
    i = 1
    for row in data_list:
        data = row.strip().split(",")
        major, courses_text, university = data[0].strip(), data[1].strip(), data[2].strip()
        u_m_id = data_dict.get(university + major)

        h_id = high_courses.get(courses_text)
        if u_m_id is not None and h_id is not None:
            correct_data.append("({},{}),".format(u_m_id, h_id))
        else:
            wrong_data.append(row.strip())
        i += 1
    correct_data.insert(0, sql)
    return correct_data, wrong_data


# 1，2
# invalid, valid = expand_2_to_3("input/高校招生专业要求 0905.csv")
#
# Utils.save_to_file(invalid, "output/高校招生专业要求-不限.csv")
# Utils.save_to_file(valid, "output/高校招生专业要求-正常数据.csv")

# 3，4,5
# data_3 = expand_str_to_list("output/高校招生专业要求-正常数据.csv")
# Utils.save_to_file(data_3, "output/高校招生专业要求-专业高中课程展开数据.csv")
# 本科
correct_data, wrong_data = convert_data_to_sql("output/高校招生专业要求-专业高中课程展开数据.csv")
Utils.save_to_file(correct_data, "output/高校专业高中课程-本科-正确.csv")
Utils.save_to_file(wrong_data, "output/高校专业高中课程-本科-错误.csv")
# 高职
correct_data, wrong_data = convert_data_to_sql("output/高校招生专业要求-专业高中课程展开数据.csv",2)
Utils.save_to_file(correct_data, "output/高校专业高中课程-专科-正确.csv")
Utils.save_to_file(wrong_data, "output/高校专业高中课程-专科-错误.csv")