from testpy import db

# Создание всех таблиц, описанных в моделях
db.create_all()

print("Database created successfully.")