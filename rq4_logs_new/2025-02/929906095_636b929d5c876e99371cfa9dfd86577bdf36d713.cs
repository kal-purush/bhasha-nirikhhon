using System.Net;
using BusinessObject.DTO;
using BusinessObject.Entities;
using BusinessObject.Interfaces;
using BusinessObject.Shares;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class VoucherController(IRepository<Voucher> _voucherRepository,
                                   IRepository<User> _userRepository,
                                   IRepository<UserVoucher> _userVoucherRepository) : ControllerBase
    {
        [HttpPost("create-voucher")]
        public async Task<IActionResult> CreateVoucher([FromBody] Voucher voucher)
        {
            Random random = new Random();
            string generatedCode;
            bool isDuplicate;

            do
            {
                generatedCode = Util.GenerateRandomString();
                isDuplicate = _voucherRepository.Find(v => v.Code == generatedCode).Any();
            } while (isDuplicate);

            Guid voucherID = Guid.NewGuid();
            Voucher createVoucher = new Voucher
            {
                Id = voucherID,
                Code = Util.GenerateRandomString(),
                Description = voucher.Description,
                Discount = voucher.Discount,
                StartDate = voucher.StartDate,
                EndDate = voucher.EndDate,
                Image = voucher.Image,
                isDeleted = false,
                QuantityUsed = voucher.QuantityUsed
            };
            await _voucherRepository.AddAsync(createVoucher);
            await _voucherRepository.SaveAsync();
            return Ok(new { Message = "Create Voucher Success" });
        }

        [HttpPut("edit-voucher/{id}")]
        public async Task<IActionResult> EditVoucher(Guid id, [FromBody] EditVoucherDTO voucherDto)
        {
            if (voucherDto == null)
            {
                return BadRequest(new { Message = "Invalid voucher data" });
            }

            var existingVoucher = await _voucherRepository.Find(v => v.Id == id).FirstOrDefaultAsync();

            if (existingVoucher == null)
            {
                return NotFound(new { Message = "Voucher not found" });
            }

            existingVoucher.Image = voucherDto.Image ?? existingVoucher.Image;
            existingVoucher.Discount = voucherDto.Discount;
            existingVoucher.Description = voucherDto.Description ?? existingVoucher.Description;
            existingVoucher.QuantityUsed = voucherDto.QuantityUsed;
            existingVoucher.StartDate = voucherDto.StartDate;
            existingVoucher.EndDate = voucherDto.EndDate;
            existingVoucher.isDeleted = voucherDto.isDeleted;

            await _voucherRepository.UpdateAsync(existingVoucher);
            await _voucherRepository.SaveAsync();

            return Ok(new { Message = "Voucher updated successfully" });
        }



        

    }
}