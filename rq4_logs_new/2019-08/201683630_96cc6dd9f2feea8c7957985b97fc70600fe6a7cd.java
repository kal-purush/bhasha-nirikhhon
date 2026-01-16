package com.intouristing.model.entity;

import com.intouristing.model.enumeration.RequestTypeEnum;
import lombok.*;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Created by Marcelo Lacroix on 27/08/19.
 */
@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class Request {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private User sender;

    private User destination;

    @Enumerated(EnumType.ORDINAL)
    private RequestTypeEnum type;

    private LocalDateTime acceptedAt;

    private LocalDateTime declinedAt;

}