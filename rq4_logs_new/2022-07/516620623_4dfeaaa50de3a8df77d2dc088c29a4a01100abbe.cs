using DrivingSchool.Model;
using DrivingSchool.Model.DatabaseModels;
using DrivingSchool.Model.DatabaseModels.SpResultModels;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace DrivingSchool.Controllers
{
    public class StudyStreamController
    {
        private readonly ApplicationDataBaseContext _context;
        public StudyStreamController()
        {
            _context = new ApplicationDataBaseContext();
        }

        public async Task<StudyStream> CreateAsync(StudyStream studyStreamVm)
        {
            if (studyStreamVm != null)
            {
                studyStreamVm.BeginDate = DateTime.Now;
                await _context.StudyStreams.AddAsync(studyStreamVm);
                await _context.SaveChangesAsync();
                return studyStreamVm;
            }
            else
                throw new ArgumentNullException();

        }
        public async Task DeleteAsync(Nullable<int> studystreamId)
        {
            if (studystreamId.HasValue)
            {
                var studyStream = await _context.StudyStreams
                    .AsNoTracking()
                    .FirstOrDefaultAsync(x => x.Id == studystreamId);
                if (studyStream != null)
                {
                    _context.StudyStreams.Remove(studyStream);
                    await _context.SaveChangesAsync();
                }
                else
                    throw new ArgumentNullException();
            }
            else
                throw new ArgumentNullException();
        }

        public async Task UpdateAsync(Nullable<int> studyStreamId, StudyStream studyStreamVm)
        {
            if (studyStreamVm != null || studyStreamId.HasValue)
            {
                var studySteam = await _context.StudyStreams
                 .FirstOrDefaultAsync(x => x.Id == studyStreamId);
                if (studySteam == null) throw new Exception("Поток не найден");

                studySteam = studyStreamVm;
                _context.StudyStreams.Update(studySteam);
                await _context.SaveChangesAsync();
            }
        }
        public async Task<List<StudyStream>> GetAsync()
        {
            return await _context.StudyStreams
                .Include(x => x.Students)
                .AsNoTracking().ToListAsync();
        }
        public async Task<List<SpStudyStream>> GetStreamsByCategory(Nullable<int> categoryId)
        {
            if (categoryId.HasValue)
            {
                SqlParameter param = new("@CategoryId", categoryId.Value);
                var streams = await _context.SpStudyStreams
                    .FromSqlRaw("GetStreamsByCategory @CategoryId", param)
                    .AsNoTracking().ToListAsync();
                return streams;
            }
            else
                throw new Exception();
        }
    }
}