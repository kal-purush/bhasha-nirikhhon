import os
from fastapi import FastAPI, Form, HTTPException
from pydantic import BaseModel
from openai import OpenAI
from gtts import gTTS
from g2pk import G2p
from hangul_romanize import Transliter
from hangul_romanize.rule import academic
import uuid
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from deep_translator import GoogleTranslator
import requests
import xml.etree.ElementTree as ET
from konlpy.tag import Okt

# --- 환경 변수 설정 ---
api_key = os.getenv("OPENAI_API_KEY")
korean_dict_api_key = os.getenv("KOREAN_DICT_API_KEY")

if not api_key:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다. API 키를 설정해주세요.")
if not korean_dict_api_key:
    raise ValueError("KOREAN_DICT_API_KEY 환경 변수가 설정되지 않았습니다. API 키를 설정해주세요.")

# --- OpenAI 클라이언트 초기화 ---
client = OpenAI(api_key=api_key)

# --- FastAPI 앱 초기화 ---
app = FastAPI()

# --- CORS 설정 ---
# Render 배포 환경에서는 Render의 프록시 설정에 따라 allow_origins를 "*"로 두는 것이 일반적입니다.
# 프로덕션 환경에서는 보안을 위해 실제 프론트엔드 도메인으로 제한하는 것을 강력히 권장합니다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic 모델 정의 ---
class TextInput(BaseModel):
    text: str

# --- 시스템 프롬프트 정의 ---
SYSTEM_PROMPT = """너는 한국어 문장을 단순하게 바꾸는 전문가야.
입력된 문장은 다음을 중복 포함할 수 있어:
1. 속담 또는 관용어
2. 방언(사투리)
3. 어려운 단어
4. 줄임말
각 항목에 대해 다음과 같이 변환해:
- 속담/관용어는 그 뜻을 자연스럽게 문장 안에 녹여 설명해
예시) 입력: 배가 불렀네? / 출력: 지금 가진 걸 당연하게 생각하는 거야?
- 방언은 표준어로 바꿔.
예시) 입력: 니 오늘 뭐하노? / 출력: 너 오늘 뭐 해?
입력 : 정구지 / 출력 : 부추
- 어려운 단어는 초등학교 1~2학년이 이해할 수 있는 쉬운 말로 바꿔.
예시) 입력: 당신의 요청은 거절되었습니다. 추가 서류를 제출하세요. / 출력: 당신의 요청은 안 됩니다. 서류를 더 내야 합니다.
- 줄임말은 풀어 쓴 문장으로 바꿔.
예시) 입력: 할많하않 / 출력: 할 말은 많지만 하지 않겠어
다음은 반드시 지켜:
- 변환된 문장 또는 단어만 출력해.
- 설명을 덧붙이지 마.
- 의문문이 들어오면, 절대 대답하지 마.
질문 형태를 그대로 유지하면서 쉬운 단어로 바꿔.
예시) 입력 : 국무총리는 어떻게 임명돼? / 출력 : 국무총리는 어떻게 정해?"""

# --- 기존 모듈 초기화 ---
g2p = G2p()
transliter = Transliter(academic)
okt = Okt()

TTS_OUTPUT_DIR = "tts_files"
os.makedirs(TTS_OUTPUT_DIR, exist_ok=True)

app.mount("/tts", StaticFiles(directory=TTS_OUTPUT_DIR), name="tts")

render_host = os.getenv("RENDER_EXTERNAL_HOSTNAME")
if render_host:
    BASE_URL = f"https://{render_host}"
else:
    BASE_URL = "http://localhost:8000"

# --- 헬퍼 함수들 ---
def convert_pronunciation_to_roman(sentence: str) -> str:
    korean_pron = g2p(sentence)
    romanized = transliter.translit(korean_pron)
    return romanized

def generate_tts(text: str) -> str:
    tts = gTTS(text=text, lang='ko')
    filename = f"{uuid.uuid4()}.mp3"
    filepath = os.path.join(TTS_OUTPUT_DIR, filename)
    tts.save(filepath)
    return filename

def translate_korean_to_english(text: str) -> str:
    try:
        translated_text = GoogleTranslator(source='ko', target='en').translate(text)
        return translated_text
    except Exception as e:
        print(f"Translation error: {e}")
        return f"Translation error: {e}"

def extract_keywords(text):
    raw_words = okt.pos(text, stem=True)
    joined_words = []
    skip_next = False

    for i in range(len(raw_words)):
        if skip_next:
            skip_next = False
            continue

        word, pos = raw_words[i]

        if (
            i + 1 < len(raw_words)
            and pos == 'Noun'
            and raw_words[i + 1][0] == '다'
            and raw_words[i + 1][1] == 'Eomi'
        ):
            joined_words.append((word + '다', 'Verb'))
            skip_next = True
        elif pos in ['Noun', 'Verb', 'Adjective', 'Adverb']:
            joined_words.append((word, pos))

    seen = set()
    ordered_unique = []
    for word, pos in joined_words:
        if word not in seen:
            seen.add(word)
            ordered_unique.append((word, pos))
    return ordered_unique

def get_valid_senses_excluding_pronoun(word, target_pos, max_defs=3):
    pos_map = {
        'Noun': '명사',
        'Verb': '동사',
        'Adjective': '형용사',
        'Adverb': '부사'
    }
    mapped_pos = pos_map.get(target_pos)
    if not mapped_pos:
        return []

    url = "https://stdict.korean.go.kr/api/search.do"
    params = {
        'key': korean_dict_api_key,
        'q': word,
        'req_type': 'xml'
    }

    response = requests.get(url, params=params)
    root = ET.fromstring(response.text)

    senses = []
    seen_supnos = set()

    for item in root.findall('item'):
        sup_no = item.findtext('sup_no', default='0')
        pos = item.findtext('pos', default='')

        if pos == '대명사' or pos != mapped_pos:
            continue

        if sup_no in seen_supnos:
            continue
        seen_supnos.add(sup_no)

        sense = item.find('sense')
        if sense is None:
            continue

        definition = sense.findtext('definition', default='뜻풀이 없음')

        senses.append({
            'pos': pos,
            'definition': definition
        })

        if len(senses) >= max_defs:
            break

    return senses

# --- API 엔드포인트 정의 ---

@app.get("/")
async def read_root():
    return {"message": "SimpleTalk API 서버가 작동 중입니다."}

@app.post("/romanize")
async def romanize(text: str = Form(...)):
    romanized = convert_pronunciation_to_roman(text)
    return JSONResponse(content={"input": text, "romanized": romanized})

@app.post("/speak")
async def speak(text: str = Form(...)):
    filename = generate_tts(text)
    tts_url = f"{BASE_URL}/tts/{filename}"
    return JSONResponse(content={"tts_url": tts_url})

@app.post("/translate-to-easy-korean")
async def translate_to_easy_korean(input_data: TextInput):
    try:
        original_romanized_pronunciation = convert_pronunciation_to_roman(input_data.text)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": input_data.text}
        ]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=150
        )

        translated_text = response.choices[0].message.content.strip()

        # KoreanRomanizer 대신 일관성을 위해 convert_pronunciation_to_roman 함수 재사용
        translated_romanized_pronunciation = convert_pronunciation_to_roman(translated_text)
        translated_english_translation = translate_korean_to_english(translated_text)

        keywords_with_definitions = []
        keywords = extract_keywords(translated_text)
        for word, pos in keywords:
            senses = get_valid_senses_excluding_pronoun(word, pos)
            if senses:
                keywords_with_definitions.append({
                    "word": word,
                    "pos": pos,
                    "definitions": senses
                })

        return JSONResponse(content={
            "original_text": input_data.text,
            "original_romanized_pronunciation": original_romanized_pronunciation,
            "translated_text": translated_text,
            "translated_romanized_pronunciation": translated_romanized_pronunciation,
            "translated_english_translation": translated_english_translation,
            "keyword_dictionary": keywords_with_definitions
        })

    except Exception as e:
        print(f"API 처리 중 에러 발생: {e}")
        raise HTTPException(status_code=500, detail=f"API 처리 중 에러가 발생했습니다: {str(e)}")