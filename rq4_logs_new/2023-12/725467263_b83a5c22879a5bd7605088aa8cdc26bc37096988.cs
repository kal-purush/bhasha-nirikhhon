using AutoMapper;
using Microsoft.EntityFrameworkCore;
using toiec_web.Infrastructure;
using toiec_web.Models;
using toiec_web.Repository.IRepository;

namespace toiec_web.Repository
{
    public class VipStudentRepository : Repository<VipStudent>, IVipStudentRepository
    {
        private readonly IUnitOfWork _uow;
        private readonly IMapper _mapper;
        public VipStudentRepository(ToiecDbContext dbContext, IUnitOfWork uow, IMapper mapper) : base (dbContext) 
        {
            _uow = uow;
            _mapper = mapper;
        }
        public Task<bool> AddVipStudent(VipStudentModel model, int duration)
        {
            try
            {
                var vipStudent = _mapper.Map<VipStudent>(model);
                vipStudent.idVipStudent = Guid.NewGuid();
                var today = DateTime.Now;
                var vipExpire = today.AddMonths(duration);
                vipStudent.vipExpire = vipExpire;
                Entities.Add(vipStudent);
                _uow.SaveChanges();
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task<bool> UpdateVipStudent(VipStudentModel model, int duration)
        {
            try
            {
                var vipStudent = await Entities.FindAsync(model.idVipStudent);
                DateTime newVipExpire = model.vipExpire.AddMonths(duration);
                vipStudent.vipExpire = newVipExpire;
                Entities.Update(vipStudent);
                _uow.SaveChanges();

                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public async Task<VipStudentModel> GetVipStudentByStudentId(string stuId)
        {
            var vipStudent = await Entities.FirstOrDefaultAsync(vStudent => vStudent.idStudent == Guid.Parse(stuId));
            return _mapper.Map<VipStudentModel>(vipStudent);
        }
    }
}