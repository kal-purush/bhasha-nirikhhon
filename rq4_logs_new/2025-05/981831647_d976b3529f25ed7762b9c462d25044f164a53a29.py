from flask import Flask, request, jsonify, send_from_directory
from PIL import Image
import numpy as np
import cv2
import os
import base64
from io import BytesIO

app = Flask(__name__, static_folder='../front', static_url_path='')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # 캐시 방지

# 이미지 디렉토리 설정
NPC_IMAGE_DIR = '../images/npc'
MONSTER_IMAGE_DIR = '../images/monster'

# 이미지 URL 요청 라우트
@app.route('/static_images/<category>/<filename>')
def serve_image(category, filename):
    image_folder = os.path.join('../images', category)
    return send_from_directory(image_folder, filename)

# 메인 페이지 라우트
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

# ORB 기반 유사도 비교 함수
def compare_orb_similarity(img1_pil, img2_pil):
    img1 = np.array(img1_pil.convert("L"))
    img2 = np.array(img2_pil.convert("L"))

    orb = cv2.ORB_create()
    kp1, des1 = orb.detectAndCompute(img1, None)
    kp2, des2 = orb.detectAndCompute(img2, None)

    if des1 is None or des2 is None or len(kp1) == 0 or len(kp2) == 0:
        return 0

    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True)
    matches = bf.match(des1, des2)

    if not matches:
        return 0

    distances = [m.distance for m in matches]
    avg_distance = sum(distances) / len(distances)
    score = 1 / (1 + avg_distance)
    return round(score, 4)

# 비교 API 라우트
@app.route('/compare', methods=['POST'])
def compare_image():
    data = request.json
    image_base64 = data['image']
    decoded = base64.b64decode(image_base64.split(",")[-1])
    uploaded_image = Image.open(BytesIO(decoded))

    results = []

    def compare_dir(directory, category):
        for filename in os.listdir(directory):
            if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                try:
                    img_path = os.path.join(directory, filename)
                    img = Image.open(img_path)
                    score = compare_orb_similarity(uploaded_image, img)

                    name_no_ext = os.path.splitext(filename)[0]
                    rel_path = f"/static_images/{category}/{filename}"

                    results.append({
                        'category': category,
                        'filename': name_no_ext,
                        'image_url': rel_path,
                        'similarity': score
                    })
                except Exception as e:
                    print(f"[비교 실패] {filename}: {e}")
                    continue

    print("\n[ORB 비교 - NPC 디렉토리]")
    compare_dir(NPC_IMAGE_DIR, 'npc')

    print("\n[ORB 비교 - MONSTER 디렉토리]")
    compare_dir(MONSTER_IMAGE_DIR, 'monster')

    # 유사도 내림차순 정렬 후 상위 5개만 반환 (0.01 이상)
    results.sort(key=lambda x: x['similarity'], reverse=True)
    filtered = [r for r in results if r['similarity'] >= 0.01][:5]

    if not filtered:
        return jsonify({
            'error': '유사한 이미지를 찾지 못했습니다.',
            'results': []
        }), 404

    return jsonify({
        'results': filtered
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)