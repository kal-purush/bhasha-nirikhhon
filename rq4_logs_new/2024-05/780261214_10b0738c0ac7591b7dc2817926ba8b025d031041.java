package org.example.canon.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.example.canon.entity.Tools;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ToolDTO {

    private List<String> tools;

    // ToolDTO의 of 메서드는 Tools 객체의 리스트를 받아서 ToolDTO 객체를 생성합니다.
    // 이 메서드는 Tools 객체의 리스트를 받아 각 객체의 tool 필드값을 추출하여 List<String>으로 만듭니다.
    public static ToolDTO of(List<Tools> toolsList) {
        List<String> tools = toolsList.stream()
                .map(Tools::getTool) // Tools 객체에서 tool 필드값을 추출
                .collect(Collectors.toList());
        return ToolDTO.builder()
                .tools(tools)
                .build();
    }
}