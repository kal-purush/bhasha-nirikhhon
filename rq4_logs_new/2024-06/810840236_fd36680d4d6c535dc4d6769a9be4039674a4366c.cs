using AutoMapper;
using ms_spa.Api.Contract.Cliente;
using ms_spa.Api.Domain.Models;
using ms_spa.Api.Domain.Repository.Interfaces;
using ms_spa.Api.Domain.Services.Interfaces;
using ms_spa.Api.Exceptions;

namespace ms_spa.Api.Domain.Services.Classes
{
    public class ClienteService(IClienteRepository clienteRepository, IMapper mapper) : IClienteService
    {
        private readonly IClienteRepository _clienteRepository = clienteRepository;
        private readonly IMapper _mapper = mapper;

        public async Task<ClienteResponseContract> Adicionar(ClienteRequestContract entidade, int idUsuario)
        {
            var cliente = _mapper.Map<Cliente>(entidade);
            cliente.UsuarioId = idUsuario;
            cliente = await _clienteRepository.Adicionar(cliente);
            return _mapper.Map<ClienteResponseContract>(cliente);
        }

        public async Task<ClienteResponseContract> Atualizar(int id, ClienteRequestContract entidade, int idUsuario)
        {
            Cliente cliente = await ObterPorIdVinculadoAoIdUsuario(id, idUsuario);
            _mapper.Map(entidade, cliente);

            cliente = await _clienteRepository.Atualizar(cliente);
            return _mapper.Map<ClienteResponseContract>(cliente);

        }

        public async Task Inativar(int id, int idUsuario)
        {
            Cliente cliente = await ObterPorIdVinculadoAoIdUsuario(id, idUsuario);
            await _clienteRepository.Deletar(cliente);
        }

        public async Task<ClienteResponseContract> ObterPorId(int id, int idUsuario)
        {
            Cliente cliente = await ObterPorIdVinculadoAoIdUsuario(id, idUsuario);
            return _mapper.Map<ClienteResponseContract>(cliente);
        }

        public async Task<int> ObterQuantidadeTotalDeClientes()
        {
            var clientes = await _clienteRepository.ObterTodos();
            return clientes.Count();
        }

        public async Task<IEnumerable<ClienteResponseContract>> ObterTodos(int idUsuario)
        {
            var cliente = await _clienteRepository.ObeterPeloIdVinculadoAoUsuario(idUsuario);
            return cliente.Select(_mapper.Map<ClienteResponseContract>);
        }

        private async Task<Cliente> ObterPorIdVinculadoAoIdUsuario(int id, int usuarioId)
        {
            var cliente = await _clienteRepository.ObterPorId(id);
            if (cliente is null || cliente.UsuarioId != usuarioId)
            {
                throw new NotFoundException($"Não foi encontrada nenhum usuário pelo id fornecido {id}");
            }

            return cliente;
        }

    }
}