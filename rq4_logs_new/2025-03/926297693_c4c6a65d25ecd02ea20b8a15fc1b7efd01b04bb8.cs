using Microsoft.EntityFrameworkCore;
using SPSS.Data;
using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Repositories.AnswerDetailRepository
{
    public class AnswerDetailRepository(AppDbContext _context) : IAnswerDetailRepository
    {
        public async Task<IEnumerable<AnswerDetail>> GetAllAsync()
        {
            return await _context.AnswerDetails
                .Include(ad => ad.Answer)
                .Include(ad => ad.AnswerSheet)
                .ToListAsync();
        }
        public async Task AddAsync(AnswerDetail answerDetail)
        {
            await _context.AnswerDetails.AddAsync(answerDetail);
            await _context.SaveChangesAsync();
        }
        public async Task UpdateAsync(AnswerDetail answerDetail)
        {
            _context.AnswerDetails.Update(answerDetail);
            await _context.SaveChangesAsync();
        }
        public async Task DeleteAsync(AnswerDetail answerDetail)
        {
            _context.AnswerDetails.Remove(answerDetail);
            await _context.SaveChangesAsync();
        }
        public async Task AddRangeAsync(List<AnswerDetail> answerDetails)
        {
            await _context.AnswerDetails.AddRangeAsync(answerDetails);
            await _context.SaveChangesAsync();
        }

    }
    
}
