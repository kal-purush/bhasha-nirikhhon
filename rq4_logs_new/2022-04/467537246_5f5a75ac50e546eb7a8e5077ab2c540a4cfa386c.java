package ripoff.facebook;

import lombok.RequiredArgsConstructor;
import org.apache.http.auth.InvalidCredentialsException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import ripoff.facebook.clients.authentication.AuthClient;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class AuthenticationFilter implements GatewayFilter {

    private final ObjectProvider<AuthClient> authClient;
    private final WebClient.Builder builder;


    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        if(!exchange.getRequest().getCookies().containsKey("JSESSIONID")) {
            throw new AuthorizationInformationNotFound("Authorization info not found.");
        }
        String sessionCookie = exchange.getRequest().getCookies().getFirst("JSESSIONID").getValue();

        if (sessionCookie.length() != 36) {
            throw new BadAuthorizationFormat("Authorization format is not correct.");
        }
        return builder.build()
                .post()
                .uri("http://localhost:8120/api/v1/auth/session?session=" + sessionCookie)
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> Mono.error(new InvalidCredentialsException("Authorization unsuccessful")))
                .bodyToMono(Long.class)
                .map(id -> {
                    exchange.getRequest()
                            .mutate()
                            .header("user-id", String.valueOf(id));
                    return exchange;
                }).flatMap(chain::filter);
    }
}