package ru.otus.integration;

import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import reactor.core.publisher.Mono;
import ru.otus.dto.BookDto;

@MessagingGateway
public interface SaveBookIntegration {

    @Gateway(requestChannel = "channel1", replyChannel = "channel2")
    Mono<BookDto> saveBook(BookDto bookDto);

}