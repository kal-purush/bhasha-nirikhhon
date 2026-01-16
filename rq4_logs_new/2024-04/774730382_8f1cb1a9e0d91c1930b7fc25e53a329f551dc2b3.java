package Happy20.GrowingPetPlant.Status;

import jakarta.persistence.*;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data // Data : getter, setter 포함
@Entity
@Table(name = "STATUS")
@NoArgsConstructor
public class Status {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) //db가 자동으로 생성(값을 직접 입력할 필요 X, pk이기 때문에)
    @Column(name = "status_number")
    private Long statusNumber;

    @Column(name = "plant_number")
    private Long plantNumber;

    private Long temperature;

    @Builder
    //인수없는 생성자 자동으로 생성됨(NoArgsConstructor), 생성자
    public Status(Long temperature, Long plantNumber) {
        this.temperature = temperature;
        this.plantNumber = plantNumber;
    }
}