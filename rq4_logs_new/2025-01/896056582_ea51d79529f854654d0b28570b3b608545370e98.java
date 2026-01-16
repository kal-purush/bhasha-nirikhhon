package PlanQ.PlanQ.embeddad;

import PlanQ.PlanQ.global.Color;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Calender {
    private String title;

    private String comment;

    @Enumerated(EnumType.STRING)
    private Color color;

    private boolean alarm;

    @Column(name = "is_clear")
    private boolean isClear;

    public void checkClear(){
        this.isClear = !isClear;
    }
}