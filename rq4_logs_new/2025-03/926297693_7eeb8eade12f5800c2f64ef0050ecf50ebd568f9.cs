using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using SPSS.Data;
using SPSS.Entities;
using SPSS.Repository.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Repositories.FeedbackRepository
{
    public class FeedbackRepository(AppDbContext _context) : IFeedbackRepository
    {
        public async Task AddAsync(Feedback f)
        {
            await _context.Feedbacks.AddAsync(f);
            await _context.SaveChangesAsync();
        }

        public async Task<IEnumerable<Feedback>> GetByProductIdAsync(int productId)
        {
            return await _context.Feedbacks
                .Where(f => f.ProductId == productId && !f.isDelete)
                .ToListAsync();
        }

        public async Task DeleteAsync(int id)
        {
            var feedback = await _context.Feedbacks.FindAsync(id);
            feedback.isDelete = true;
            await _context.SaveChangesAsync();  
        }

        public async Task<IEnumerable<Feedback>> GetAllAsync()
        {
            return await _context.Feedbacks.Where(f => !f.isDelete).ToListAsync();
        }

        public async Task<Feedback> GetByIdAsync(int id)
        {
            return await _context.Feedbacks.FirstOrDefaultAsync(f => f.Id == id );
        }

        public async Task UpdateAsync(Feedback f)
        {
            _context.Feedbacks.Update(f);
            await _context.SaveChangesAsync();
        }

       
    }
}