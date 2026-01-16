package com.iot7.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

@Getter
@Setter
@Entity
@Table(name = "MENU_COMBINATION")
public class MenuCombination {

    @EmbeddedId
    private MenuCombinationId id;

    @MapsId("menuId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "MENU_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Menu menu;

    @MapsId("userId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "USER_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.NO_ACTION) // USER는 삭제 제한
    private User user;

    @MapsId("pairedMenuId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "PAIRED_MENU_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Menu pairedMenu;

    @Size(max = 1000)
    @Column(name = "COMBINATION_CONTENT", length = 1000)
    private String combinationContent;
}