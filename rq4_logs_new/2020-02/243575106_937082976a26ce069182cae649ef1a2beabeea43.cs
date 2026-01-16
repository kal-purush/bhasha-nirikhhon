using System.Collections.Generic;
using System.Threading.Tasks;
using file.Core;
using file.Core.Models;
using file.Core.Services;

namespace file.Services
{
    public class AttachmentService : IAttachmentService
    {
        private readonly IUnitOfWork _unitOfWork;

        public AttachmentService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task DeleteAttachment(Attachment attachment)
        {
            _unitOfWork.Attachments.Remove(attachment);
            await _unitOfWork.Commit();
        }

        public async Task<Attachment> GetAttachmentById(int id)
        {
            return await _unitOfWork.Attachments.GetByIdAsync(id);
        }

        public async Task<IEnumerable<Attachment>> GetAttachmentsByUserId(int userId)
        {
            return await _unitOfWork.Attachments.GetAttachmentsByUserId(userId);
        }

        public async Task<IEnumerable<Attachment>> GetAttachmentsWithUser()
        {
            return await _unitOfWork.Attachments.GetAllAttachment();
        }

        public async Task<Attachment> UploadAttachment(Attachment attachment)
        {
            await _unitOfWork.Attachments.AddAsync(attachment);
            await _unitOfWork.Commit();
            return attachment;
        }
    }
}