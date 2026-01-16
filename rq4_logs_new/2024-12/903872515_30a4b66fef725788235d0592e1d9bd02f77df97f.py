import sqlite3

# Conexión a la base de datos
conn = sqlite3.connect('files.db')
cursor = conn.cursor()

# Eliminar todos los registros de la tabla 'usuarios'
cursor.execute("DELETE FROM files")

# Confirmar cambios
conn.commit()

print("Tabla 'archivos' limpiada exitosamente.")

# Cerrar la conexión
conn.close()