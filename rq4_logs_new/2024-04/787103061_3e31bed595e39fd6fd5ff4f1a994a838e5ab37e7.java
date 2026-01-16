package br.com.fiap.fiapfood.application.usecase;

import br.com.fiap.fiapfood.domain.repository.UsuarioGateway;
import br.com.fiap.fiapfood.domain.usecase.UsuarioUsecase;
import br.com.fiap.fiapfood.domain.model.UsuarioDomain;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class UsuarioUseCaseImp implements UsuarioUsecase {

    private final UsuarioGateway usuarioGateway;

    @Override
    public UsuarioDomain salvar(UsuarioDomain usuario) {
        return usuarioGateway.salvar(usuario);
    }

    @Override
    public UsuarioDomain buscarPorId(String id) {
        return usuarioGateway.buscarPorId(id);
    }

    @Override
    public List<UsuarioDomain> findAll() {
        return usuarioGateway.findAll();
    }

    @Override
    public void deletar(Long id) {
        usuarioGateway.deletar(id);
    }

    @Override
    public UsuarioDomain findByCpf(String cpf) {
        return usuarioGateway.findByCpf(cpf);
    }
}