using System;
using System.Linq;
using System.Threading.Tasks;
using FFXIVVenues.Veni.Middleware;
using FFXIVVenues.Veni.Persistance.Abstraction;
using FFXIVVenues.Veni.Services;
using NChronicle.Core.Interfaces;

namespace FFXIVVenues.Veni.Auditing;

internal class AuditingService
{
    private readonly IApiService _apiService;
    private readonly Task _activeAuditTask;
    private readonly IRepository _repository;
    private readonly IChronicle _chronicle;

    public AuditingService(IApiService apiService, IRepository repository, IChronicle chronicle)
    {
        this._apiService = apiService;
        this._repository = repository;
        this._chronicle = chronicle;
    }

    public async Task StartAsync()
    {
        if (this._activeAuditTask != null)
            return;

        var activeAudits = await this._repository.GetWhere<Audit>(a => a.Status == AuditStatus.Active);
        var audit = activeAudits.FirstOrDefault();
        if (audit == null)
        {
            audit = new Audit();
        }
        var allVenues = this._apiService.GetAllVenuesAsync();
        
        return 
    }
    
}

public class Audit : IEntity
{
    public string id { get; }
    public AuditStatus Status { get; set; }
    public AuditRecord VenueAudit { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }

    public Audit() =>
        this.id = DateTime.Now.ToString("yyyyMMddHHmm");

}

public record AuditRecord(string VenueId, VenueAuditStatus Status, DateTime StatusSetAt);

public enum AuditStatus
{
    Inactive,
    Active,
    Complete
}

public enum VenueAuditStatus
{
    AwaitingResponse,
    RespondedEdit,
    RespondedConfirmed,
    RespondedDelete
}
