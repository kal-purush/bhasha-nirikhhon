package moofarm.ssuckssuck.domain.oauth.service;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import moofarm.ssuckssuck.domain.oauth.authcode.AuthCodeRequestUrlProviderComposite;
import moofarm.ssuckssuck.domain.oauth.client.OauthMemberClientComposite;
import moofarm.ssuckssuck.domain.oauth.domain.OauthMember;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.domain.oauth.domain.repository.OauthMemberRepository;
import moofarm.ssuckssuck.domain.oauth.presentation.dto.request.OauthLoginRequest;
import moofarm.ssuckssuck.domain.oauth.presentation.dto.response.OauthLoginResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OauthService {

    private final AuthCodeRequestUrlProviderComposite authCodeRequestUrlProviderComposite;
    private final OauthMemberClientComposite oauthMemberClientComposite;
    private final OauthMemberRepository oauthMemberRepository;

    public String getAuthCodeRequestUrl(OauthServerType oauthServerType) {
        return authCodeRequestUrlProviderComposite.provide(oauthServerType);
    }

    @Transactional
    public OauthLoginResponse login(OauthLoginRequest oauthLoginRequest, HttpServletResponse response) {
        OauthMember oauthMember = oauthMemberClientComposite.fetch(oauthLoginRequest.oauthServerType(), oauthLoginRequest.code());
        OauthMember saved = oauthMemberRepository.findByOauthServerTypeAndEmail(oauthMember.getOauthServerType(), oauthMember.getEmail())
                .orElseGet(() -> oauthMemberRepository.save(oauthMember));

        return new OauthLoginResponse(saved.getOauthMemberInfo(), false);
    }
}