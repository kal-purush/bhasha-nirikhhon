import cv2
import numpy as np
import tkinter as tk
from tkinter import ttk
import mediapipe as mp

# Загрузка модели MediaPipe Face Mesh
mp_face_mesh = mp.solutions.face_mesh
face_mesh = mp_face_mesh.FaceMesh(static_image_mode=False, max_num_faces=1, min_detection_confidence=0.5,
                                  min_tracking_confidence=0.5)

# Загрузка масок
mask_left = cv2.imread('mask_left.png', -1)
mask_right = cv2.imread('mask_right.png', -1)

# Запуск видеопотока с камеры
video_capture = cv2.VideoCapture(0)

# Настройки положения маски относительно исходной точки
mask_x_offset_left = 0
mask_y_offset_left = 0
mask_x_offset_right = 0
mask_y_offset_right = 0

# Параметры для сглаживания
alpha = 0.5
smoothed_left_eye_center = None
smoothed_right_eye_center = None

# Переменная для включения/отключения черного экрана
show_black_screen = False
mask_scale = 2
prev_crop_height_percent_left = 0
crop_offset_vertical = 0

# Переменная для отображения ключевых точек
show_landmarks = False


def apply_exponential_smoothing(current_center, smoothed_center, alpha=0.5):
    if smoothed_center is None:
        return current_center
    smoothed_x = alpha * current_center[0] + (1 - alpha) * smoothed_center[0]
    smoothed_y = alpha * current_center[1] + (1 - alpha) * smoothed_center[1]
    return (int(smoothed_x), int(smoothed_y))


def apply_exponential_smoothing_float(current_value, smoothed_value, alpha=0.5):
    if smoothed_value is None:
        return current_value
    smoothed_value = alpha * current_value + (1 - alpha) * smoothed_value
    return smoothed_value

def toggle_black_screen():
    global show_black_screen
    show_black_screen = not show_black_screen


def toggle_landmarks():
    global show_landmarks
    show_landmarks = not show_landmarks


# Создание окна tkinter
root = tk.Tk()
root.title("Mask Adjustment")


# Создание кнопки для переключения ключевых точек
landmarks_button = ttk.Button(root, text="Toggle Landmarks", command=toggle_landmarks)
landmarks_button.pack(pady=10)

# Создание кнопки для переключения черного экрана
black_screen_button = ttk.Button(root, text="Toggle Black Screen", command=toggle_black_screen)
black_screen_button.pack(pady=10)


while True:
    ret, frame = video_capture.read()
    if not ret:
        break

    # Зеркальное отображение кадра
    frame = cv2.flip(frame, 1)

    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    results = face_mesh.process(frame_rgb)

    if show_black_screen:
        frame = np.zeros_like(frame)

    if results.multi_face_landmarks:
        for face_idx, face_landmarks in enumerate(results.multi_face_landmarks):
            # Индексы вершин для левого глаза
            left_eye_indices = [33, 133, 157, 158, 159, 160, 144, 145, 153, 154, 130, 247, 398, 380, 381, 382, 362,
                                263, 387, 388, 374]
            # Индексы вершин для правого глаза
            right_eye_indices = [362, 398, 384, 385, 386, 387, 263, 390, 374, 375, 247, 263, 33, 133, 157, 158, 159,
                                 160, 144, 145, 153, 154, 130]

            # Извлечение координат точек левого и правого глаза
            left_eye = np.array([(int(face_landmarks.landmark[i].x * frame.shape[1]),
                                  int(face_landmarks.landmark[i].y * frame.shape[0])) for i in left_eye_indices])
            right_eye = np.array([(int(face_landmarks.landmark[i].x * frame.shape[1]),
                                   int(face_landmarks.landmark[i].y * frame.shape[0])) for i in right_eye_indices])

            # Координаты точек для вычисления расстояния
            left_point_38 = (
            int(face_landmarks.landmark[159].x * frame.shape[1]), int(face_landmarks.landmark[159].y * frame.shape[0]))
            left_point_42 = (
            int(face_landmarks.landmark[145].x * frame.shape[1]), int(face_landmarks.landmark[145].y * frame.shape[0]))
            left_point_41 = (
            int(face_landmarks.landmark[153].x * frame.shape[1]), int(face_landmarks.landmark[153].y * frame.shape[0]))
            right_point_48 = (
            int(face_landmarks.landmark[386].x * frame.shape[1]), int(face_landmarks.landmark[386].y * frame.shape[0]))
            right_point_47 = (
            int(face_landmarks.landmark[374].x * frame.shape[1]), int(face_landmarks.landmark[374].y * frame.shape[0]))

            # Вычисление расстояния между точками
            distance_42_38 = np.linalg.norm(np.array(left_point_42) - np.array(left_point_38))
            distance_42_41 = np.linalg.norm(np.array(left_point_42) - np.array(left_point_41))
            distance_right = np.linalg.norm(np.array(right_point_48) - np.array(right_point_47))

            # Вычисление центра глаз (берем середину контура)
            left_eye_center = np.array((int(face_landmarks.landmark[159].x * frame.shape[1]),
                                        int(face_landmarks.landmark[159].y * frame.shape[0])))
            right_eye_center = np.array((int(face_landmarks.landmark[386].x * frame.shape[1]),
                                         int(face_landmarks.landmark[386].y * frame.shape[0])))

            # Применение сглаживания к центру глаз
            smoothed_left_eye_center = apply_exponential_smoothing(left_eye_center, smoothed_left_eye_center, alpha)
            smoothed_right_eye_center = apply_exponential_smoothing(right_eye_center, smoothed_right_eye_center, alpha)

            # Вычисление угла поворота головы
            delta_x = smoothed_right_eye_center[0] - smoothed_left_eye_center[0]
            delta_y = smoothed_right_eye_center[1] - smoothed_left_eye_center[1]
            theta = np.arctan2(delta_y, delta_x)
            angle = np.degrees(theta)
            angle = -angle


            # --- Левый глаз ---
            # Вычисление ширины и высоты глаза (берем от крайних точек)
            eye_width = int(np.linalg.norm(np.array((int(face_landmarks.landmark[133].x * frame.shape[1]),
                                                     int(face_landmarks.landmark[133].y * frame.shape[0]))) - np.array((
                                                                                                                       int(
                                                                                                                           face_landmarks.landmark[
                                                                                                                               362].x *
                                                                                                                           frame.shape[
                                                                                                                               1]),
                                                                                                                       int(
                                                                                                                           face_landmarks.landmark[
                                                                                                                               362].y *
                                                                                                                           frame.shape[
                                                                                                                               0])))))
            eye_height = int(np.linalg.norm(np.array((int(face_landmarks.landmark[159].x * frame.shape[1]),
                                                      int(face_landmarks.landmark[159].y * frame.shape[0]))) - np.array(
                (int(face_landmarks.landmark[158].x * frame.shape[1]),
                 int(face_landmarks.landmark[158].y * frame.shape[0])))))

            # Масштабирование маски пропорционально размеру глаза
            mask_width, mask_height = mask_left.shape[:2]
            scale_factor = min(eye_width / mask_width, eye_height / mask_height)
            scaled_mask_left = cv2.resize(mask_left, None, fx=scale_factor * mask_scale,
                                          fy=scale_factor * mask_scale)
            # Обрезание маски сверху в зависимости от расстояния
            crop_height_percent_left = 0
            if distance_42_38 < distance_42_41:
                crop_height_percent_left = (1 - (distance_42_38 / distance_42_41))  # Процент обрезки
                crop_height_percent_left = min(crop_height_percent_left, 1)  # Ограничим процент обрезки до 1

            # Применяем сглаживание к проценту обрезки
            crop_height_percent_left = apply_exponential_smoothing_float(crop_height_percent_left,
                                                                         prev_crop_height_percent_left, alpha)
            prev_crop_height_percent_left = crop_height_percent_left

            # Вычисляем кол-во пикселей для обрезки, теперь учитывая смещение
            crop_height_pixels_left = int(scaled_mask_left.shape[0] * crop_height_percent_left) + crop_offset_vertical

            # Проверка границ crop_height_pixels
            crop_height_pixels_left = max(0, min(crop_height_pixels_left, scaled_mask_left.shape[0]))

            # Применяем обрезку
            if crop_height_pixels_left > 0:
                cropped_mask_left = scaled_mask_left[crop_height_pixels_left:, :, :]
            else:
                cropped_mask_left = scaled_mask_left[:scaled_mask_left.shape[0] + crop_height_pixels_left, :, :]

            if cropped_mask_left.size == 0:
                continue


            # Поворот маски
            rotation_matrix_left = cv2.getRotationMatrix2D(
                (cropped_mask_left.shape[1] / 2, cropped_mask_left.shape[0] / 2),
                angle, 1)
            rotated_mask_left = cv2.warpAffine(cropped_mask_left, rotation_matrix_left,
                                               (cropped_mask_left.shape[1], cropped_mask_left.shape[0]))

            # Вычисление смещения маски с использованием сглаженного центра
            x_offset_left = int(smoothed_left_eye_center[0] - rotated_mask_left.shape[1] / 2 + mask_x_offset_left)
            y_offset_left = int(smoothed_left_eye_center[1] - rotated_mask_left.shape[0] / 2 + mask_y_offset_left)

            # Наложение маски на кадр
            mask_height_left, mask_width_left = rotated_mask_left.shape[:2]
            y_end_left = min(y_offset_left + mask_height_left, frame.shape[0])
            x_end_left = min(x_offset_left + mask_width_left, frame.shape[1])

            mask_rgb_left = rotated_mask_left[0:y_end_left - y_offset_left, 0:x_end_left - x_offset_left, :3]
            mask_alpha_left = rotated_mask_left[0:y_end_left - y_offset_left, 0:x_end_left - x_offset_left, 3] / 255.0

            for c in range(3):
                frame[y_offset_left:y_end_left, x_offset_left:x_end_left, c] = (
                        mask_alpha_left * mask_rgb_left[:, :, c] +
                        (1 - mask_alpha_left) * frame[y_offset_left:y_end_left, x_offset_left:x_end_left, c]
                )

            # --- Правый глаз ---
            # Вычисление ширины и высоты глаза (берем от крайних точек)
            eye_width = int(np.linalg.norm(np.array((int(face_landmarks.landmark[386].x * frame.shape[1]),
                                                     int(face_landmarks.landmark[386].y * frame.shape[0]))) - np.array((
                                                                                                                       int(
                                                                                                                           face_landmarks.landmark[
                                                                                                                               263].x *
                                                                                                                           frame.shape[
                                                                                                                               1]),
                                                                                                                       int(
                                                                                                                           face_landmarks.landmark[
                                                                                                                               263].y *
                                                                                                                           frame.shape[
                                                                                                                               0])))))
            eye_height = int(np.linalg.norm(np.array((int(face_landmarks.landmark[386].x * frame.shape[1]),
                                                      int(face_landmarks.landmark[386].y * frame.shape[0]))) - np.array(
                (int(face_landmarks.landmark[385].x * frame.shape[1]),
                 int(face_landmarks.landmark[385].y * frame.shape[0])))))

            # Масштабирование маски пропорционально размеру глаза
            mask_width, mask_height = mask_right.shape[:2]
            scale_factor = min(eye_width / mask_width, eye_height / mask_height)
            scaled_mask_right = cv2.resize(mask_right, None, fx=scale_factor * mask_scale,
                                           fy=scale_factor * mask_scale)

            # Поворот маски
            rotation_matrix_right = cv2.getRotationMatrix2D(
                (scaled_mask_right.shape[1] / 2, scaled_mask_right.shape[0] / 2), angle, 1)
            rotated_mask_right = cv2.warpAffine(scaled_mask_right, rotation_matrix_right,
                                                (scaled_mask_right.shape[1], scaled_mask_right.shape[0]))

            # Вычисление смещения маски с использованием сглаженного центра
            x_offset_right = int(smoothed_right_eye_center[0] - rotated_mask_right.shape[1] / 2 + mask_x_offset_right)
            y_offset_right = int(smoothed_right_eye_center[1] - rotated_mask_right.shape[0] / 2 + mask_y_offset_right)

            # Наложение маски на кадр
            mask_height_right, mask_width_right = rotated_mask_right.shape[:2]
            y_end_right = min(y_offset_right + mask_height_right, frame.shape[0])
            x_end_right = min(x_offset_right + mask_width_right, frame.shape[1])

            if rotated_mask_right.size == 0:
                continue

            mask_rgb_right = rotated_mask_right[0:y_end_right - y_offset_right, 0:x_end_right - x_offset_right, :3]
            mask_alpha_right = rotated_mask_right[0:y_end_right - y_offset_right, 0:x_end_right - x_offset_right,
                               3] / 255.0

            for c in range(3):
                frame[y_offset_right:y_end_right, x_offset_right:x_end_right, c] = (
                        mask_alpha_right * mask_rgb_right[:, :, c] +
                        (1 - mask_alpha_right) * frame[y_offset_right:y_end_right, x_offset_right:x_end_right, c]
                )

            # Рисуем ключевые точки и их номера, если show_landmarks = True
            if show_landmarks:
                for idx, landmark in enumerate(face_landmarks.landmark):
                    x = int(landmark.x * frame.shape[1])
                    y = int(landmark.y * frame.shape[0])
                    cv2.circle(frame, (x, y), 2, (0, 255, 0), -1)  # Рисуем точку
                    cv2.putText(frame, str(idx), (x + 5, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.3, (0, 255, 0),
                                1)  # Рисуем номер

            # Отображение расстояния между точками на кадре
            cv2.putText(frame, f"Dist 42-38: {distance_42_38:.2f}", (20, 30), cv2.FONT_HERSHEY_SIMPLEX, 0.7,
                        (255, 255, 255), 2)
            cv2.putText(frame, f"Dist 42-41: {distance_42_41:.2f}", (20, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.7,
                        (255, 255, 255), 2)
            cv2.putText(frame, f"Right Dist: {distance_right:.2f}", (20, 90), cv2.FONT_HERSHEY_SIMPLEX, 0.7,
                        (255, 255, 255), 2)

    cv2.imshow('Video', frame)
    root.update()

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

video_capture.release()
cv2.destroyAllWindows()
root.destroy()