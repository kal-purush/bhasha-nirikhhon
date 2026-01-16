using SPSS.Data;
using SPSS.Entities;
using Microsoft.EntityFrameworkCore;

namespace SPSS.Repository.Repositories.AnswerRepository
{
    public class AnswerRepository(AppDbContext _context) : IAnswerRepository
    {
        public async Task AddAsync(Answer a)
        {
            await _context.Answers.AddAsync(a);
            await _context.SaveChangesAsync();
        }
        public async Task DeleteAsync(Answer a)
        {
            await _context.Answers.AddAsync(a);
            await _context.SaveChangesAsync();
        }
        public async Task<IEnumerable<Answer>> GetAllAsync()
        {
            return await _context.Answers
                .Where(a => !a.isDelete)  // Chỉ lấy những bản ghi chưa bị xóa mềm
                .Include(a => a.Question)
                .ToListAsync();
        }
        public async Task UpdateAsync(Answer a)
        {
            _context.Answers.Update(a);
            await _context.SaveChangesAsync();
        }
        // Cập nhật isDelete = true (Soft Delete)
        public async Task SoftDeleteAsync(int id)
        {
            var answer = await _context.Answers.FindAsync(id);
            if (answer != null)
            {
                answer.isDelete = true;
                await _context.SaveChangesAsync();
            }
        }

        // Cập nhật isDelete = false (Restore)
        public async Task RestoreAsync(int id)
        {
            var answer = await _context.Answers.FindAsync(id);
            if (answer != null)
            {
                answer.isDelete = false;
                await _context.SaveChangesAsync();
            }
        }
    }
}