import axios from "axios";

// const REFRESH_URL = "/users/refresh"; // 리플래쉬 토큰 앤드포인트 경로
const API_SERVER = import.meta.env.VITE_API_SERVER; // baseURL

const useCustomAxios = () => {
  // const user = {};
  // 인스턴스 생성 -> http 공통 필드 저장
  const instance = axios.create({
    baseURL: API_SERVER,
    timeout: 1000 * 3,
    headers: {
      "content-type": "application/json;charset=UTF-8",
      accept: "application/json",
    },
    // 쿠키, Authorization 인증 헤더, TLS client certifacates를 내포하는 자격 증명을 전송 가능하도록 설정
    withCredentials: true,
  });

  // 요청 인터셉트 -> 요청이 전달되기 전에 작업 수행, 요청 오류가 있는 작업 수행을 할 수 있는 2개의 콜백 함수를 받음
  instance.interceptors.request.use();

  // 응답 인터셉티 -> 응답 데이터가 있는 작업 수행, 응답 오류가 있는 작업 수행을 할 수 있는 2개의 콜백 함수를 받음
  instance.interceptors.response.use();
};

export default useCustomAxios;