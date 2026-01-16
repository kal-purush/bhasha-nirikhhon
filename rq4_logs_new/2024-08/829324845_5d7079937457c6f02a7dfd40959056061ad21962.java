package pintoss.giftmall.domains.user.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import pintoss.giftmall.common.enums.UserRole;
import pintoss.giftmall.domains.user.domain.User;

@Getter
@NoArgsConstructor
public class OAuthRegisterRequest {

    @NotBlank(message = "이메일은 필수 항목입니다.")
    @Email(message = "이메일 형식에 맞게 입력해주세요.")
    private String email;

    @NotBlank(message = "이름은 필수 항목입니다.")
    private String name;

    @NotBlank(message = "전화번호는 필수 항목입니다.")
    private String phone;

    private String inflow;

    @Builder
    public OAuthRegisterRequest(String email,String name, String phone, String inflow) {
        this.email = email;
        this.name = name;
        this.phone = phone;
        this.inflow = inflow;
    }

    public User toEntity() {
        return User.builder()
                .email(this.email)
                .name(this.name)
                .phone(this.phone)
                .role(UserRole.USER)
                .inflow(this.inflow)
                .build();
    }

}