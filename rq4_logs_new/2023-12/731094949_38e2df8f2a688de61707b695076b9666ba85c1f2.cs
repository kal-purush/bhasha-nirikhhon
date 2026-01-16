using System.Transactions;
using AutoMapper;
using QRisto.Application.Errors;
using QRisto.Application.Models.Request.Provider;
using QRisto.Application.Models.Response.Provider;
using QRisto.Application.Utils;
using QRisto.Persistence;
using QRisto.Persistence.Entity;
using QRisto.Persistence.Repositories.Implementations;

namespace QRisto.Application.Services.Provider;

public class ProviderService : IProviderService
{
    private readonly UnitOfWork _unitOfWork;
    private readonly IMapper _mapper;

    public ProviderService(IMapper mapper, UnitOfWork unitOfWork)
    {
        _mapper = mapper;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<ProviderResponse>> CreateAsync(ProviderPostRequest providerPostRequest)
    {
        using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, 
                   new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }))
        {
            try
            {
                var providerEntity = _mapper.Map<ProviderEntity>(providerPostRequest);
                providerEntity = await _unitOfWork.ProviderRepository.InsertAsync(providerEntity);
                _unitOfWork.ProviderRepository.Save();
                
                var model = _mapper.Map<ProviderResponse>(providerEntity);
                scope.Complete();
                return Result<ProviderResponse>.Success(model);
            }
            catch (Exception ex)
            {
                return Result<ProviderResponse>.Failure(ProviderErrors.UnableCreateProvider, ex.ToString());
            }
        }
    }
}