package ru.practicum.shareit.item;

import lombok.*;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
public class ItemDtoMini {

    private Integer id;
    private String name;
    private Integer ownerId;
    private String description;
    private Boolean available;
    private Integer requestId;
}