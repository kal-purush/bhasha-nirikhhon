package com.project.quanlycanghangkhong.dto.response.teams;

import com.project.quanlycanghangkhong.dto.TeamDTO;
import com.project.quanlycanghangkhong.dto.response.ApiResponseCustom;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Schema(description = "API response for all teams, data is List<TeamDTO>", required = true)
public class ApiAllTeamsResponse extends ApiResponseCustom<List<TeamDTO>> {
}