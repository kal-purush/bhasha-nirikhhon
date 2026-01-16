import tkinter as tk
import platform
import psutil
import wmi

def get_info():
    # Получаем информацию о системе
    c = wmi.WMI()
    motherboard = c.Win32_BaseBoard()[0]
    processor = c.Win32_Processor()[0]
    memory = round(psutil.virtual_memory().total / (1024 ** 3), 2)
    memory_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=1)
    gpu_name = c.Win32_VideoController()[0].Name
    gpu_percent = psutil.virtual_memory().percent

    # Информация о платформе
    windows_info = platform.platform()
    windows_version = platform.version()
    username = platform.node()

    # Обновляем содержимое виджетов
    system_label.configure(text=f"Материнская плата: {motherboard.Product}", anchor="w")
    cpu_label.configure(text=f"Процессор: {processor.Name} ({cpu_percent}% загрузки)", anchor="w")
    gpu_label.configure(text=f"Видео-карта: {gpu_name} ({gpu_percent}% загрузки)", anchor="w")
    memory_label.configure(text=f"Оперативная память: {memory} ГБ ({memory_percent}% использовано)", anchor="w")
    windows_label.configure(text=f"Windows: {windows_info} (Версия: {windows_version}, Пользователь: {username})", anchor="w")

    # Жесткий диск
    disk_info = psutil.disk_partitions()
    disk_count = 0
    disk_total = 0
    disk_used = 0
    for part in disk_info:
        if 'cdrom' in part.opts or part.fstype == '':
            continue
        partition_usage = psutil.disk_usage(part.mountpoint)
        disk_count += 1
        disk_total += round(partition_usage.total / (1024.0 ** 3), 2)
        disk_used += round(partition_usage.used / (1024.0 ** 3), 2)

    disk_percent = round((disk_used / disk_total) * 100, 2)
    disk_label.configure(text="Диски: {}% занято ({} дисков, занято {} ГБ из {} ГБ)".format(disk_percent, disk_count, disk_used, disk_total), anchor="w")

# Создаем главное окно приложения
root = tk.Tk()
root.title("Информация о ПК 1.0.2 by f0bas")
root.geometry("600x300")  # Увеличиваем размер окна

# Создаем кнопку для отображения информации
info_button = tk.Button(root, text="Показать информацию о ПК", command=get_info, bg="yellow", font=("Arial Bold", 14), width=50, height=1)
info_button.pack(pady=10)

# Создаем рамку для отображения информации
info_frame = tk.LabelFrame(root, text="", bd=0, font=("Arial Bold", 12))
info_frame.pack(pady=10)

# Добавляем виджеты для отображения информации
system_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
system_label.pack(pady=5, padx=5, fill="x")

cpu_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
cpu_label.pack(pady=5, padx=5, fill="x")

gpu_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
gpu_label.pack(pady=5, padx=5, fill="x")

memory_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
memory_label.pack(pady=5, padx=5, fill="x")

windows_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
windows_label.pack(pady=5, padx=5, fill="x")

disk_label = tk.Label(info_frame, font=("Arial", 10), highlightthickness=1)
disk_label.pack(pady=5, padx=5, fill="x")

# Запускаем главный цикл обработки событий
root.mainloop()