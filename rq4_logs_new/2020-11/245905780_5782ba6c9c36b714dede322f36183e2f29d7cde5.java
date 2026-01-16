package hyve.petshow.facade;

import hyve.petshow.controller.converter.PrestadorConverter;
import hyve.petshow.controller.converter.ServicoDetalhadoConverter;
import hyve.petshow.controller.representation.ServicoDetalhadoRepresentation;
import hyve.petshow.exceptions.NotFoundException;
import hyve.petshow.service.port.PrestadorService;
import hyve.petshow.service.port.ServicoDetalhadoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;

@Component
public class ServicoDetalhadoFacade {
    @Autowired
    private ServicoDetalhadoService servicoDetalhadoService;
    @Autowired
    private PrestadorService prestadorService;
    @Autowired
    private ServicoDetalhadoConverter servicoDetalhadoConverter;
    @Autowired
    private PrestadorConverter prestadorConverter;

    public Page<ServicoDetalhadoRepresentation> buscarServicosDetalhadosPorTipoServico(Integer id, Pageable pageable) throws Exception {
        var servicosDetalhados = servicoDetalhadoConverter.toRepresentationPage(
                servicoDetalhadoService.buscarServicosDetalhadosPorTipoServico(id, pageable));

        for (ServicoDetalhadoRepresentation servico : servicosDetalhados) {
            var prestador = prestadorConverter.toRepresentation(
                    prestadorService.buscarPorId(servico.getPrestadorId()));

            servico.setPrestador(prestador);
        }

        return servicosDetalhados;
    }

    public ServicoDetalhadoRepresentation buscarPorPrestadorIdEServicoId(Long prestadorId, Long servicoId) throws Exception {
        var servico = servicoDetalhadoService.buscarPorPrestadorIdEServicoId(prestadorId, servicoId);
        var prestador = prestadorConverter.toRepresentation(
                prestadorService.buscarPorId(servico.getPrestadorId()));
        var representation = servicoDetalhadoConverter.toRepresentation(servico);

        representation.setPrestador(prestador);

        return representation;
    }
}