/**
 * result_code : string
 *
 * - 200 : 성공
 * - 300 : 필수 필드 누락
 * - 301 : 잘못된 코드 값
 * - 302 : 잘못된 변수 값
 * - 601 : 인증 번호가 다를 시 (회원가입)
 */

/**
 *  회원 가입
 *  user_id : uuid
 *
 *  로그인
 *  user_id : uuid
 *  user_email: string
 *  access_token: string
 *  + user_nickname : string
 *  + profile_image
 *  + 게임 기록 데이터 (랭킹...)
 *
 *  소셜 로그인
 *  auth_code: string
 */

export type Resonse = {
	result_code: string;
};

export type User = {
	user_id: string; //uuid
	user_email: string;
	user_nickname: string;
};

export type ResponseLogin = Resonse & User;

export type LoginRequest = {
	user_email: string;
	user_password: string;
};

export type SignUpRequest = {
	user_email: string;
	user_password: string;
	user_nickname: string;
	email_certification: number;
};