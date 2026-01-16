package tech.munidigital.lavadero.mappers;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import tech.munidigital.lavadero.dto.request.ClienteRequestDTO;
import tech.munidigital.lavadero.dto.response.ClienteResponseDTO;
import tech.munidigital.lavadero.dto.response.VehiculoResponseDTO;
import tech.munidigital.lavadero.entity.Cliente;
import tech.munidigital.lavadero.entity.Vehiculo;
import java.util.List;

@Mapper(componentModel = "spring")
public interface ClienteMapper {

    ClienteResponseDTO toDto(Cliente cliente);

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "vehiculos", ignore = true)
    Cliente toEntity(ClienteRequestDTO dto);

    @Mapping(target = "clienteId", source = "cliente.id")
    List<VehiculoResponseDTO> toVehiculoDtoList(List<Vehiculo> vehiculos);

}