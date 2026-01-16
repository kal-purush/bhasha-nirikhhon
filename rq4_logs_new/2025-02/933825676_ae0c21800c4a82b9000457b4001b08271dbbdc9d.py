import cv2
import mediapipe as mp
import socket

# Configurações do socket TCP
HOST = '10.0.1.4'  # Ou o IP do seu Raspberry Pi
PORT = 8080  # Porta que você definiu no código C

mp_drawing = mp.solutions.drawing_utils
mp_drawing_styles = mp.solutions.drawing_styles
mp_hands = mp.solutions.hands

cap = cv2.VideoCapture(0)

ultimo_dedos_levantados = -1

with mp_hands.Hands(
    model_complexity=0,
    max_num_hands=1,
    min_detection_confidence=0.5,
    min_tracking_confidence=0.5) as hands:

    while cap.isOpened():
        success, image = cap.read()
        if not success:
            continue

        image.flags.writeable = False
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        results = hands.process(image)

        image.flags.writeable = True
        image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)

        if results.multi_hand_landmarks:
            for hand_landmarks in results.multi_hand_landmarks:
                mp_drawing.draw_landmarks(
                    image,
                    hand_landmarks,
                    mp_hands.HAND_CONNECTIONS,
                    mp_drawing_styles.get_default_hand_landmarks_style(),
                    mp_drawing_styles.get_default_hand_connections_style())

                dedos_levantados = 0

                # Contagem de dedos (mais robusta)
                polegar_cima = hand_landmarks.landmark[mp_hands.HandLandmark.THUMB_TIP].x > hand_landmarks.landmark[mp_hands.HandLandmark.THUMB_MCP].x
                if polegar_cima:
                    dedos_levantados += 1

                if hand_landmarks.landmark[mp_hands.HandLandmark.INDEX_FINGER_TIP].y < hand_landmarks.landmark[mp_hands.HandLandmark.INDEX_FINGER_DIP].y:
                    dedos_levantados += 1
                if hand_landmarks.landmark[mp_hands.HandLandmark.MIDDLE_FINGER_TIP].y < hand_landmarks.landmark[mp_hands.HandLandmark.MIDDLE_FINGER_DIP].y:
                    dedos_levantados += 1
                if hand_landmarks.landmark[mp_hands.HandLandmark.RING_FINGER_TIP].y < hand_landmarks.landmark[mp_hands.HandLandmark.RING_FINGER_DIP].y:
                    dedos_levantados += 1
                if hand_landmarks.landmark[mp_hands.HandLandmark.PINKY_TIP].y < hand_landmarks.landmark[mp_hands.HandLandmark.PINKY_DIP].y:
                    dedos_levantados += 1


                # Envia 5 se todos os dedos estiverem para cima
                if dedos_levantados == 5:
                    enviar_valor = 5
                else:
                    enviar_valor = dedos_levantados
                    
                # Envia dados via TCP (dentro da verificação de alteração)
                if enviar_valor != ultimo_dedos_levantados: # Agora verifica enviar_valor
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((HOST, PORT))
                            s.sendall(str(enviar_valor).encode()) # Envia enviar_valor
                            print(f"Dados enviados: {enviar_valor}")
                            ultimo_dedos_levantados = enviar_valor  # Atualiza após o envio
                    except Exception as e:
                        print(f"Erro ao enviar dados: {e}")

                cv2.putText(image, str(dedos_levantados), (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2, cv2.LINE_AA)

        cv2.imshow('MediaPipe Hands', cv2.flip(image, 1))
        if cv2.waitKey(5) & 0xFF == 27:
            break

cap.release()
cv2.destroyAllWindows()