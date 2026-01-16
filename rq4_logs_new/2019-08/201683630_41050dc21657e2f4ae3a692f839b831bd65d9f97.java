package com.intouristing.model.dto;

import com.intouristing.model.entity.Request;
import com.intouristing.model.entity.User;
import com.intouristing.model.enumeration.RelationshipTypeEnum;
import lombok.*;

import java.time.LocalDateTime;
import java.util.Optional;

import static java.util.Objects.nonNull;

/**
 * Created by Marcelo Lacroix on 27/08/2019.
 */
@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestDTO {

    private Long id;

    private Long sender;

    private Long destination;

    private RelationshipTypeEnum type;

    private LocalDateTime createdAt;

    private LocalDateTime acceptedAt;

    private LocalDateTime declinedAt;

    public static RequestDTO parseDTO(Request request) {
        if (nonNull(request)) {
            RequestDTO
                    .builder()
                    .id(request.getId())
                    .sender(Optional.ofNullable(request.getSender()).map(User::getId).orElse(null))
                    .destination(Optional.ofNullable(request.getDestination()).map(User::getId).orElse(null))
                    .type(request.getType())
                    .createdAt(request.getCreatedAt())
                    .acceptedAt(request.getAcceptedAt())
                    .declinedAt(request.getDeclinedAt())
                    .build();
        }

        return null;
    }
}