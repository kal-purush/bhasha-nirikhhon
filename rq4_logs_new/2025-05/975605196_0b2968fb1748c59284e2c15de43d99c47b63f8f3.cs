using FluentValidation;
using static dotnet9_ketnoigiaothuong.Domain.Contracts.CompanyContract;

namespace dotnet9_ketnoigiaothuong.Domain.Validators
{
    public class CreateCompanyModelValidator : AbstractValidator<CreateCompanyModel>
    {
        public CreateCompanyModelValidator() 
        {
            RuleFor(x => x.CompanyName).NotEmpty().WithMessage("Tên công ty không được để trống");
            RuleFor(x => x.TaxCode).NotEmpty().WithMessage("Mã số thuế không được để trống");
            RuleFor(x => x.BusinessSector).NotEmpty().WithMessage("Lĩnh vực kinh doanh không được để trống");
            RuleFor(x => x.Address).NotEmpty().WithMessage("Địa chỉ không được để trống");
            RuleFor(x => x.Representative).NotEmpty().WithMessage("Người đại diện không được để trống");
            RuleFor(x => x.Email).NotEmpty().WithMessage("Email không được để trống")
                .EmailAddress().WithMessage("Email không đúng định dạng");
            RuleFor(x => x.PhoneNumber).NotEmpty().WithMessage("Số điện thoại không được để trống")
                .Matches(@"^\d{10,11}$").WithMessage("Số điện thoại phải có 10-11 chữ số");
        }
    }
} 