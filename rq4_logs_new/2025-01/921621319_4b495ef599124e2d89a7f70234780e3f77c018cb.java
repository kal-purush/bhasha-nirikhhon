package com.metamong.mt.global.apispec;

/**
 * <p>에러 코드를 정의한 모든 enum 클래스가 구현해야 할 인터페이스.
 * 에러 코드를 정의한 enum 클래스들이 이 인터페이스를 구현함으로써 ErrorResponse에서
 * 다형성을 통해 쉽게 에러 응답을 만들어낼 수 있다.
 */
public interface ErrorCodeSpecifiable {

    /**
     * 에러의 이름.
     * 주로 enum의 이름이 된다.
     * @return 에러 이름
     */
    public String name();
    
    /**
     * 응답 코드.
     * 
     * @return 응답 코드. 401, 403, 404, 500 등
     */
    public int getStatusCode();
    
    /**
     * <p>에러 코드. 응답 코드만으로는 우리 도메인의 모든 에러를 표현할 수 없으므로
     * 에러 코드를 따로 정의할 필요가 있다.
     * 
     * <p>에러 코드 예시:<br>
     * - 회원 관련 에러코드: MEM001, MEM002, ...
     * - 관리자 관련 에러 코드: ADMIN001, ADMIN002, ...
     * 
     * @return 에러 코드
     */
    public String getErrorCode();
    
    /**
     * <p>에러 메시지를 반환한다.
     * 
     * @return 해당 에러를 설명하는 에러 메시지
     */
    public String getMessage();
}