using Application.Repositories;
using Domain.Entities;
using MediatR;

namespace Application.Commands
{
    public class SeedDatas
    {
        public record Command : IRequest<SeedResponse>
        {
            public string Name1 { get; init; } = default!;
            public string Name2 { get; init; } = default!;
            public string InitiatorChandaNo { get; set; } = default!;
        }

        public record SeedResponse(List<Guid> Ids);

        public class Handler : IRequestHandler<Command, SeedResponse>
        {
            private readonly IUnitOfWork _unitOfWork;
            private readonly IMemberRepository _memRepo;
            private readonly IJamaatRepository _jaRepo;
            private readonly ICircuitRepository _circuitRepo;
            private readonly IChandaTypeRepository _chandaTypeRepo;


            public Handler(IUnitOfWork unitOfWork, IMemberRepository repo, IJamaatRepository jaRepo, ICircuitRepository circuitRepo, IChandaTypeRepository chandaTypeRepo)
            {
                _unitOfWork = unitOfWork;
                _memRepo = repo;
                _jaRepo = jaRepo;
                _circuitRepo = circuitRepo;
                _chandaTypeRepo = chandaTypeRepo;
            }

            public async Task<SeedResponse?> Handle(Command req, CancellationToken cancellationToken)
            {
                var circuit1 = new Circuit(req.Name1, "ABK", "0001");
                var circuit2 = new Circuit(req.Name2, "IBD", "0001");

                var jamaat1 = new Jamaat("Lafiaji", "ABK-LAFIAJI", circuit1.Id, "0001");
                var jamaat2 = new Jamaat("Camp-Obantoko", "ABK-OBANTOKO", circuit1.Id, "0001");
                var jamaat3 = new Jamaat("Oke-Abetu", "ABK-OKE-ABETU", circuit1.Id, "0001");
                var jamaat4 = new Jamaat("Omi-Adio", "IBD-OMI-ADIO", circuit2.Id, "0001");
                var jamaat5 = new Jamaat("Apata", "IBD-APATA", circuit2.Id, "0001");


                var mlId = Guid.NewGuid();
                var mlId1 = Guid.NewGuid();
                var mlId2 = Guid.NewGuid();
                var mlId3 = Guid.NewGuid();
                var mlId4 = Guid.NewGuid();
                var member = new Member("0001", "Ade Ola", "adeola@example.com", "0801111111", jamaat1.Id, mlId, "0001");
                var member1 = new Member("0002", "Aina Segun", "segun@example.com", "0801111112", jamaat1.Id, mlId1, "0001");
                var member2 = new Member("0003", "Akin Ola", "akin@example.com", "0801111113", jamaat2.Id, mlId2, "0001");
                var member3 = new Member("0004", "Opeyemi Alore", "alore@example.com", "0801111114", jamaat2.Id, mlId3, "0001");
                var member4 = new Member("0005", "Ahmad Yekin", "ahmad@example.com", "0801111115", jamaat1.Id, mlId4, "0001");

                var ml = new MemberLedger(member.Id, "0001");
                var ml1 = new MemberLedger(member1.Id, "0001");
                var ml2 = new MemberLedger(member2.Id, "0001");
                var ml3 = new MemberLedger(member3.Id, "0001");
                var ml4 = new MemberLedger(member4.Id, "0001");

                var ct = new ChandaType("Chanda Arm", "CHA", "Chanda Arm", Guid.NewGuid(), "0001");
                var ct1 = new ChandaType("Chanda Wasiyyat", "CHW", "Chanda Wasiyyat", Guid.NewGuid(), "0001");
                var ct2 = new ChandaType("Welfare", "WEL", "Welfare", Guid.NewGuid(), "0001");

                try
                {
                    await _circuitRepo.AddAsync(circuit2);
                    await _circuitRepo.AddAsync(circuit1);

                    await _jaRepo.AddAsync(jamaat1);
                    await _jaRepo.AddAsync(jamaat2);
                    await _jaRepo.AddAsync(jamaat3);
                    await _jaRepo.AddAsync(jamaat4);
                    await _jaRepo.AddAsync(jamaat5);

                    await _memRepo.CreateAsync(member);
                    await _memRepo.CreateAsync(member1);
                    await _memRepo.CreateAsync(member2);
                    await _memRepo.CreateAsync(member3);
                    await _memRepo.CreateAsync(member4);

                    await _chandaTypeRepo.AddAsync(ct);
                    await _chandaTypeRepo.AddAsync(ct1);
                    await _chandaTypeRepo.AddAsync(ct2);

                    await _unitOfWork.SaveChangesAsync();

                    return new SeedResponse(new List<Guid>() { circuit1.Id, circuit2.Id, jamaat1.Id, jamaat2.Id, jamaat3.Id, jamaat4.Id, jamaat5.Id, member.Id, member1.Id, member2.Id, member3.Id, member4.Id, ct.Id, ct1.Id, ct2.Id });
                }
                catch (Exception ex)
                {
                    var err = ex.Message;
                }
                return new SeedResponse(new List<Guid>());
            }
        }

    }
}