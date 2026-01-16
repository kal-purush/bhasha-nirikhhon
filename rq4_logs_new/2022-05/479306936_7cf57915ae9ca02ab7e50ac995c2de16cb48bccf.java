package wiseSWlife.wiseSWlife.domain.login.loginServiceInterface;

import wiseSWlife.wiseSWlife.domain.member.Member;

public interface LoginService {
    Member login(String loginId, String password);
}