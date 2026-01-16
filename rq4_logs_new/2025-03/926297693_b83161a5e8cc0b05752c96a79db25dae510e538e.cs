using Microsoft.EntityFrameworkCore;
using SPSS.Data;
using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Repositories.AnswerSheetRepository
{
    public class AnswerSheetRepository(AppDbContext _context) : IAnswerSheetRepository
    {
        public async Task<IEnumerable<AnswerSheet>> GetAllAsync()
        {
            return await _context.AnswerSheets
                .Where(a => !a.isDelete)
                .Include(a => a.AppUser)
                .ToListAsync();
        }
        public async Task AddAsync(AnswerSheet answerSheet)
        {
            await _context.AnswerSheets.AddAsync(answerSheet);
            await _context.SaveChangesAsync();
        }
        public async Task UpdateAsync(AnswerSheet answerSheet)
        {
            _context.AnswerSheets.Update(answerSheet);
            await _context.SaveChangesAsync();
        }
        public async Task DeleteAsync(AnswerSheet answerSheet)
        {
            _context.AnswerSheets.Remove(answerSheet);
            await _context.SaveChangesAsync();
        }
        public async Task SoftDeleteAsync(int id)
        {
            var answerSheet = await _context.AnswerSheets.FindAsync(id);
            if (answerSheet != null)
            {
                answerSheet.isDelete = true;
                await _context.SaveChangesAsync();
            }           
        }
        public async Task RestoreAsync(int id)
        {
            var answerSheet = await _context.AnswerSheets.FindAsync(id);
            if (answerSheet != null)
            {
                answerSheet.isDelete = false;
                await _context.SaveChangesAsync();
            }
        }
    }
}