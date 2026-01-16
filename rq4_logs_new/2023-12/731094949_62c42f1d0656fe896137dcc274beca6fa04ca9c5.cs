using AutoMapper;
using QRisto.Application.Models.Request.Table;
using QRisto.Application.Models.Response.Table;
using QRisto.Application.Utils;
using QRisto.Persistence.Entity.Provider;
using QRisto.Persistence.Repositories.Implementations;

namespace QRisto.Application.Services.Table;

public class TableService : ITableService
{
    private readonly IMapper _mapper;
    private readonly UnitOfWork _unitOfWork;

    public TableService(UnitOfWork unitOfWork, IMapper mapper)
    {
        _unitOfWork = unitOfWork;
        _mapper = mapper;
    }

    public async Task<Result<TableResponseModel>> CreateTableAsync(CreateTableRequestModel requestModel)
    {
        try
        {
            var service = await _unitOfWork.ServiceRepository.GetByIdAsync(requestModel.ServiceId);

            if (service == null)
            {
                return Result<TableResponseModel>.Failure($"Service with ID {requestModel.ServiceId} does not exist.");
            }

            var table = _mapper.Map<TableEntity>(requestModel);

            var createdTable = await _unitOfWork.TableRepository.InsertAsync(table);

            var result = _mapper.Map<TableResponseModel>(createdTable);

            return Result<TableResponseModel>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<TableResponseModel>.Failure(ex.ToString());
        }
    }
}