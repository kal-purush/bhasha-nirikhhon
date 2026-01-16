using MediatR;
using Standard.Tool.Platform.Common.Helper;
using Standard.Tool.Platform.Data.Entities;
using Standard.Tool.Platform.Data.Import;
using Standard.Tool.Platform.Data.Infrastructure;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Materials
{
    public record CreateMaterialDataCommand(EditMaterialRequest Payload) : IRequest<ImportResult>;

    public class CreateDataCommandHandler : IRequestHandler<CreateMaterialDataCommand, ImportResult>
    {
        private readonly IRepository<MaterialEntity> _repo;

        public CreateDataCommandHandler(IRepository<MaterialEntity> repo) => _repo = repo;

        public async Task<ImportResult> Handle(CreateMaterialDataCommand request, CancellationToken ct)
        {
            var insertResult = new ImportResult();
            if (request.Payload.MaterialDataList.Any())
            {
                var dtData = TransHelper<Material, MaterialEntity>.TransAutoCopyList(request.Payload.MaterialDataList.ToList());

                await _repo.DeleteAsync(ct);
                insertResult.Succeeded = await _repo.AddRangeAsync(dtData.ToList()) > 0 ? true : false;
                insertResult.Message = insertResult.Succeeded ? string.Empty : "";
            }

            return await Task.FromResult(insertResult);
        }
    }
}