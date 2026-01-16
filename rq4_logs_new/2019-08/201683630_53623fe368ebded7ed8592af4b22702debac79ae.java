package com.intouristing.intouristing.controller.websocket;

import com.intouristing.intouristing.model.dto.ErrorWsDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Created by Marcelo Lacroix on 17/08/2019.
 */
@Slf4j
@ControllerAdvice
@RestController
public class ApiValidatorWsExceptionHandler {

    private final String LOOKUP_DESTINATION = "lookupDestination";
    private final String STOMP_COMMAND = "stompCommand";

    @MessageExceptionHandler
    @SendTo("/error")
    public final ErrorWsDTO handleWebSocketExceptions(Exception ex, Message message) {
        ErrorWsDTO errorDetails = buildErrorDetails(ex, message);

        return errorDetails;
    }

    private ErrorWsDTO buildErrorDetails(Exception ex, Message message) {
        log.error(ex.getMessage(), ex);

        String lookupDestination = (String) message.getHeaders().get(LOOKUP_DESTINATION),
                msg = ex.getMessage(),
                stompCommand = Optional.ofNullable((StompCommand) message.getHeaders().get(STOMP_COMMAND))
                        .map(StompCommand::getMessageType)
                        .map(SimpMessageType::name)
                        .orElse(null);

        return ErrorWsDTO
                .builder()
                .timestamp(LocalDateTime.now())
                .lookupDestination(lookupDestination)
                .message(msg)
                .stompCommand(stompCommand)
                .build();
    }

}