using System.ComponentModel.DataAnnotations;
using AutoMapper;
using ImpulsionaTech.Contas.Application.EventSourcedNormalizers;
using ImpulsionaTech.Contas.Application.Interfaces;
using ImpulsionaTech.Contas.Application.ViewModels;
using ImpulsionaTech.Contas.Domain.Interfaces;

namespace ImpulsionaTech.Contas.Application.Services
{
  public class ContasAppService : IContasAppService
  {
    private readonly IMapper _mapper;
    private readonly IContasRepository _contasRepository;

    public void Dispose()
    {
      throw new NotImplementedException();
    }

    public Task<IEnumerable<ClienteViewModel>> GetAll()
    {
      throw new NotImplementedException();
    }

    public Task<IList<ClienteHistoryData>> GetAllHistory(Guid id)
    {
      throw new NotImplementedException();
    }

    public Task<ClienteViewModel> GetById(Guid id)
    {
      throw new NotImplementedException();
    }

    public Task<ValidationResult> Register(ClienteViewModel clienteViewModel)
    {
      throw new NotImplementedException();
    }

    public Task<ValidationResult> Remove(Guid id)
    {
      throw new NotImplementedException();
    }

    public Task<ValidationResult> Update(ClienteViewModel clienteViewModel)
    {
      throw new NotImplementedException();
    }
  }
}