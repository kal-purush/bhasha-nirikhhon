using System.Threading.Tasks;
using file.Core;
using file.Core.Repositories;
using file.Persistance.Repositories;

namespace file.Persistance
{
    public class UnitOfWork : IUnitOfWork
    {
        private readonly ApiDbContext _context;
        private UserRepository _userRepository;
        private AttachmentRepository _attachmentRepository;

        public UnitOfWork(ApiDbContext context)
        {
            _context = context;
        }

        public IUserRepository Users => _userRepository = _userRepository ?? new UserRepository(_context);
        public IAttachmentRepository Attachments => _attachmentRepository = _attachmentRepository ?? new AttachmentRepository(_context);

        public async Task<int> Commit()
        {
            return await _context.SaveChangesAsync();
        }

        public void Dispose()
        {
            _context.Dispose();
        }
    }
}