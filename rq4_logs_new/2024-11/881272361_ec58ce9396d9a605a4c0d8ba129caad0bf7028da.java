package AIWA.McpBackend.controller.api.dto.cloudrouter;

import lombok.Getter;

import java.util.List;

@Getter
public class CloudRouterDto {
    private String routerName;
    private String network;
    private String routerRegion;
    private String encryptedInterconnectRouter;
    private String googleAsn;
    private String interconnectVpnGateway;
    private String connection;
    private String bgpSession;
    private List<RouterNatDto> routerNatDtos; // NAT 정보를 담을 리스트 추가

    // 생성자
    public CloudRouterDto(String routerName, String network, String routerRegion,
                          String encryptedInterconnectRouter, String googleAsn,
                          String interconnectVpnGateway, String connection,
                          String bgpSession, List<RouterNatDto> routerNatDtos) {
        this.routerName = routerName;
        this.network = network;
        this.routerRegion = routerRegion;
        this.encryptedInterconnectRouter = encryptedInterconnectRouter;
        this.googleAsn = googleAsn;
        this.interconnectVpnGateway = interconnectVpnGateway;
        this.connection = connection;
        this.bgpSession = bgpSession;
        this.routerNatDtos = routerNatDtos;
    }
}