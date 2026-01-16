package co.com.playlist.playlist.controller.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

@Getter
@Setter
@Builder
public class PlaylistData {

    @NotEmpty
    @NonNull
    private String nombre;

    @NotBlank
    @NotNull
    private String descripcion;

    @NotNull
    private List<SongData> canciones;
}