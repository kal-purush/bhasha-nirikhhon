using Application.DTOs.AuthorDto;
using Database.Databases;
using Domain.Models;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Authors.Commands.CreateAuthor
{
    public class CreateAuthorCommand : IRequest<Author>
    {
        private CreateAuthorDto createAuthorDto;

        public CreateAuthorCommand(Author authorToAdd)
        {
            NewAuthor = authorToAdd;
        }

        public CreateAuthorCommand(CreateAuthorDto createAuthorDto)
        {
            this.createAuthorDto = createAuthorDto;
        }

        public Author NewAuthor { get; }
    }
}