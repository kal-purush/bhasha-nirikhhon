import { GET, POST } from '@/utils/axios';

// 면접보길 원하는 카테고리 리스트 전송받은 후 인터뷰 id 반환
export const signIn = async (email: string, password: string) => {
    return await POST(`/accounts/signin`, { email: email, password: password });
};

// 특정 모의면접 상세정보 반환
export const signUp = async (name: string, email: string, password: string) => {
    return await POST(`/accounts/signup`, {
        name: name,
        email: email,
        password: password,
    });
};

// 특정 모의면접에서 한가지 질문에 대한 답변 Mp3 파일 전송
export const getProfile = async () => {
    return await GET(`/accounts/profile`);
};