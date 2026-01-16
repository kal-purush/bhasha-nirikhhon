package project.TadeM.Order.entity;


import java.time.LocalDateTime;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Order {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	Long id;

	long productId;
	String userId;

	Long payPrice; // 결제 금액
	String status; // 거래 신청중 - 거래 진행중 - 거래 완료

	LocalDateTime regDt; // 신청일



}