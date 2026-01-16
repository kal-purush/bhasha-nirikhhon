# test_clova_dubbing.py
import os
import requests
from dotenv import load_dotenv
import time

# .env 파일 로드
load_dotenv()

# API 키 확인
api_key_id = os.environ.get('NAVER_API_KEY_ID')
api_secret = os.environ.get('NAVER_API_SECRET')

print(f"API 키 ID: {'설정됨 - ' + api_key_id[:4] + '...' if api_key_id else '설정되지 않음'}")
print(f"API 시크릿: {'설정됨 - ' + api_secret[:4] + '...' if api_secret else '설정되지 않음'}")

# 더빙 API 테스트
if api_key_id and api_secret:
    # 프리미엄 TTS API
    url = "https://naveropenapi.apigw.ntruss.com/tts-premium/v1/tts"
    
    headers = {
        "X-NCP-APIGW-API-KEY-ID": api_key_id.strip(),
        "X-NCP-APIGW-API-KEY": api_secret.strip(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "speaker": "nara",  # 더빙 API의 화자 (nara, jinho, nhajun, ndaeman, nsunkyung 등)
        "volume": "0",
        "speed": "0",
        "pitch": "0",
        "text": "안녕하세요, 네이버 클로바 더빙 API 테스트입니다.",
        "format": "mp3"
    }
    
    try:
        print("API 호출 시도 중...")
        response = requests.post(url, headers=headers, data=data)
        
        print(f"응답 상태 코드: {response.status_code}")
        
        if response.status_code == 200:
            # 테스트 파일 저장
            with open("test_dubbing.mp3", 'wb') as f:
                f.write(response.content)
            print("테스트 성공! test_dubbing.mp3 파일이 생성되었습니다.")
        else:
            print(f"API 호출 실패: {response.text}")
    except Exception as e:
        print(f"오류 발생: {e}")
else:
    print("API 키가 설정되지 않았습니다.")

def generate_voice_clova(text, output_path, speaker="nara", volume=0, pitch=0, speed=0, retry_count=3, retry_delay=2):
    """네이버 Clova 더빙 API로 음성 생성"""
    # API 키 가져오기
    api_key_id = os.environ.get('NAVER_API_KEY_ID')
    api_secret = os.environ.get('NAVER_API_SECRET')
    
    # 디버깅: API 키 확인
    print(f"API 키 ID: {'설정됨 - ' + api_key_id[:4] + '...' if api_key_id else '설정되지 않음'}")
    print(f"API 시크릿: {'설정됨 - ' + api_secret[:4] + '...' if api_secret else '설정되지 않음'}")
    
    if not api_key_id or not api_secret:
        logger.error("네이버 API 키가 설정되지 않았습니다. 환경 변수 NAVER_API_KEY_ID와 NAVER_API_SECRET를 설정하세요.")
        return False
    
    # 더빙 API 엔드포인트 사용
    url = "https://naveropenapi.apigw.ntruss.com/voice-premium/v1/tts"
    
    headers = {
        "X-NCP-APIGW-API-KEY-ID": api_key_id.strip(),
        "X-NCP-APIGW-API-KEY": api_secret.strip(),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # 발음 교정 적용
    text = apply_pronunciation_corrections(text)
    
    # 디버깅: 텍스트 확인
    print(f"변환할 텍스트 (일부): {text[:50]}...")
    
    data = {
        "speaker": speaker,
        "volume": str(volume),
        "speed": str(speed),
        "pitch": str(pitch),
        "text": text,
        "format": "mp3"
    }
    
    for attempt in range(retry_count):
        try:
            print(f"API 호출 시도 {attempt+1}/{retry_count}...")
            response = requests.post(url, headers=headers, data=data)
            
            # 디버깅: 응답 상태 확인
            print(f"응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                print(f"음성 파일 생성 완료: {output_path}")
                return True
            else:
                print(f"API 호출 실패: {response.text}")
                if attempt < retry_count - 1:
                    print(f"{retry_delay}초 후 재시도합니다...")
                    time.sleep(retry_delay)
                else:
                    print(f"최대 재시도 횟수 초과. 음성 변환 실패: {output_path}")
                    return False
        except Exception as e:
            print(f"예상치 못한 오류 발생: {e}")
            if attempt < retry_count - 1:
                print(f"{retry_delay}초 후 재시도합니다...")
                time.sleep(retry_delay)
            else:
                return False
    
    return False

def main():
    try:
        # 환경 변수에서 설정 가져오기 (기본값 제공)
        speaker = os.environ.get('TTS_SPEAKER', 'nara')  # 더빙 API 화자 (nara, jinho 등)
        speed = int(os.environ.get('TTS_SPEED', '0'))    # 더빙 API는 -5~5 범위 사용
        pitch = int(os.environ.get('TTS_PITCH', '0'))    # 더빙 API는 -5~5 범위 사용
        volume = int(os.environ.get('TTS_VOLUME', '0'))  # 더빙 API는 -5~5 범위 사용
        output_dir = os.environ.get('TTS_OUTPUT_DIR', 'audio')
        
        # ... 나머지 코드 ...
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()