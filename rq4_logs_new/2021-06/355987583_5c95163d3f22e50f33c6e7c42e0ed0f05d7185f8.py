from services.db_service import DbService

from scipy.sparse import lil_matrix

from sklearn.preprocessing import normalize
from scipy.sparse import spdiags

from scipy.sparse import vstack
import numpy as np

import json


db_service = DbService()

# Получение вакансий
vacancies_id = db_service.execute_script("""
    SELECT v.id
    FROM vacancies as v INNER JOIN vacancy_skill as v_s ON v.id = v_s.vacancy_id
    GROUP BY v.id
    HAVING count(v_s.vacancy_id) > 2
""")
vacancy_to_col = {}
for col_id, (vacancy_id,) in enumerate(vacancies_id):
    vacancy_to_col[vacancy_id] = col_id

# Получение ключевых навыков
skills = db_service.execute_script("""
    SELECT DISTINCT skill_name
    FROM vacancy_skill
    GROUP BY skill_name
    HAVING count(vacancy_id) > 25
""")
skill_to_row = {}
for col_id, (skill_name,) in enumerate(skills):
    skill_to_row[skill_name] = col_id

print('Количество вакансий', len(vacancy_to_col))
print('Количество навыков', len(skill_to_row))
print()

# ----------------------------------------------------------------------------------------------------------------------

vacancy_skill = db_service.execute_script("""
    SELECT *
    FROM vacancy_skill
""")
matrix = lil_matrix((len(skill_to_row), len(vacancy_to_col)),)

# Создание матрицы skill-vacancy
for vacancy, skill in vacancy_skill:
    row_id = skill_to_row.get(skill)
    col_id = vacancy_to_col.get(vacancy)
    if row_id is not None and col_id is not None:
        matrix[row_id, col_id] = 1

percent = float(matrix.nnz) / len(skill_to_row) / len(vacancy_to_col) * 100
print(u"Процент заполненности матрицы: %.2f%%" % percent)
print()

# ----------------------------------------------------------------------------------------------------------------------
# Нормализация
normalized_matrix = normalize(matrix.tocsr()).tocsr()
# Вычисление скалярного произведения
cosine_sim_matrix = normalized_matrix.dot(normalized_matrix.T)
# обнуляем диагональ, чтобы исключить ее из рекомендаций
# быстрое обнуление диагонали
diag = spdiags(-cosine_sim_matrix.diagonal(), [0], *cosine_sim_matrix.shape, format='csr')
cosine_sim_matrix = cosine_sim_matrix + diag

percent = float(cosine_sim_matrix.nnz) / cosine_sim_matrix.shape[0] / cosine_sim_matrix.shape[1] * 100
print(u"Процент заполненности матрицы: %.2f%%" % percent)
print(u"Размер в МБ:", cosine_sim_matrix.data.nbytes / 1024 / 1024)
print()

# ----------------------------------------------------------------------------------------------------------------------
cosine_sim_matrix = cosine_sim_matrix.tocsr()
m = 30

# построим top-m матрицу в один поток
rows = []
for row_id in np.unique(cosine_sim_matrix.nonzero()[0]):
    row = cosine_sim_matrix[row_id]  # исходная строка матрицы
    if row.nnz > m:
        work_row = row.tolil()
        # заменяем все top-m элементов на 0, результат отнимаем от row
        # при большом количестве столбцов данная операция работает быстрее,
        # чем простое зануление всех элементов кроме top-m
        work_row[0, row.nonzero()[1][np.argsort(row.data)[-m:]]] = 0
        row = row - work_row.tocsr()
    rows.append(row)
topk_matrix = vstack(rows)
# нормализуем матрицу-результат
topk_matrix = normalize(topk_matrix)

# Сохранение полученной матрицы
with open('colab_filtration/topk_matrix', 'wb') as file:
    np.save(file, topk_matrix.toarray())
with open('colab_filtration/topk_matrix', 'rb') as file:
    mat = np.load(file)
    topk_matrix = lil_matrix(mat)

percent = float(topk_matrix.nnz) / topk_matrix.shape[0] / topk_matrix.shape[1] * 100
print(u"Процент заполненности матрицы: %.2f%%" % percent)
print(u"Размер в МБ:", topk_matrix.data.nbytes / 1024 / 1024)

# ----------------------------------------------------------------------------------------------------------------------

# индекс для преобразования row_id -> skill, где row_id - идентификатор навыка в матрице
row_to_skill = {row_id: skill for skill, row_id in skill_to_row.items()}

# Запись в файл
with open('colab_filtration/row_to_skill', 'w') as file:
    json.dump(row_to_skill, file)
with open('colab_filtration/row_to_skill', 'r') as file:
    row_to_skill = json.load(file)
    row_to_skill = {int(row): skill for row, skill in row_to_skill.items()}
    skill_to_row = {skill: row for row, skill in row_to_skill.items()}

# user_skills = ['Python', 'PostgreSQL', 'Git', ' SQL', 'C++', 'Pandas']
#
#
# def vectorise(skills):
#     user_vector = lil_matrix((len(skill_to_row), 1),)
#     for s in skills:
#         if skill_to_row.get(s) is None:
#             continue
#         user_vector[skill_to_row[s], 0] = 1
#     return user_vector
#
#
# user_vector = vectorise(user_skills).tocsr()
#
# # ----------------------------------------------------------------------------------------------------------------------
#
# # 1. перемножить матрицу item-item и вектор рейтингов пользователя A
# x = topk_matrix.dot(user_vector).tolil()
# # 2. занулить ячейки, соответствующие навыкам, которые пользователь A уже оценил
# for i, j in zip(*user_vector.nonzero()):
#     x[i, j] = 0
#
# # превращаем столбец результата в вектор
# x = x.T.tocsr()
#
# # 3. отсортировать навыки в порядке убывания значений и получить top-k рекомендаций (quorum = 5)
# quorum = 5
# data_ids = np.argsort(x.data)[-quorum:][::-1]
#
# result = []
# for arg_id in data_ids:
#     row_id, p = x.indices[arg_id], x.data[arg_id]
#     result.append({"obj_id": row_to_skill[row_id], "weight": p})
#
# print(result)