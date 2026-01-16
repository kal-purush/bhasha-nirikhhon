package br.com.serratec.ecommerce.service;

import java.util.List;

import br.com.serratec.ecommerce.dto.usuario.UsuarioRequestDTO;
import br.com.serratec.ecommerce.dto.usuario.UsuarioResponseDTO;
// import br.com.serratec.ecommerce.service.CRUDService;

public class TesteService implements CRUDService<UsuarioRequestDTO, UsuarioResponseDTO> {

    @Override
    public List<UsuarioResponseDTO> obterTodos() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'obterTodos'");
    }

    @Override
    public UsuarioResponseDTO obterPorId(long id) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'obterPorId'");
    }

    @Override
    public UsuarioResponseDTO adicionar(UsuarioRequestDTO request) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'adicionar'");
    }

    @Override
    public UsuarioResponseDTO atualizar(long id, UsuarioRequestDTO requestDto) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'atualizar'");
    }

    @Override
    public void deletar(long id) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deletar'");
    }

}