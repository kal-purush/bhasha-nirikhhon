package com.example.campuschool_backend.dto.lecture;

import com.example.campuschool_backend.domain.lecture.Register;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RegisterDTO {
    private Long id;
    private String img;
    private String name;
    private String description;
    public static RegisterDTO from(Register register) {
        RegisterDTO registerDTO = new RegisterDTO();
        registerDTO.setId(register.getId());
        registerDTO.setImg(register.getUser().getImg());
        registerDTO.setName(register.getUser().getName());
        registerDTO.setDescription(register.getUser().getDescription().getDescription());
        return registerDTO;
    }
}