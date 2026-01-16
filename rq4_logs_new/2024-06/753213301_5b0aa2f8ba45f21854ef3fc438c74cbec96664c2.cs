using Amazon.Runtime.Internal;
using amazon_backend.CQRS.Commands.RewiewTagRequests;
using amazon_backend.Data.Entity;
using amazon_backend.Models;
using FluentValidation;
using MediatR;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using amazon_backend.Data;
using amazon_backend.Data.Model;

namespace amazon_backend.CQRS.Commands.CategoryPropertyKeyRequests
{
    public class CreateCategoryPropertyKeyCommandRequst : IRequest<Result<CategoryPropertyKeyProfile>>
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public bool IsFilter { get; set; }
        public bool IsRequired { get; set; }
        public bool IsDeleted { get; set; }
        public string NameCategory { get; set; }
        public uint CategoryId {  get; set; }

    }
    public class CreateCategoryPropertyKeyValidator : AbstractValidator<CreateCategoryPropertyKeyCommandRequst>
    {
       
        public CreateCategoryPropertyKeyValidator()
        {
            RuleFor(x => x.Name).NotEmpty();
            RuleFor(x => x.NameCategory).NotEmpty();
            
        }
    }
}