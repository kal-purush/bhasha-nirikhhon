using BookCatalog.Core.Wrappers;
using System.Text.Json;

namespace BookCatalog.Infrastructure.Mappers
{
    public static class AuthorMapper
    {
        public static Core.Models.Author ToDomainModel (this Models.JsonEntities.Author entity)
            => new Core.Models.Author(entity.FirstName, entity.LastName, entity.BirthDate);

        public static Core.Models.Author ToDomainModel(this Models.XmlEntities.Author entity)
            => new Core.Models.Author(entity.FirstName, entity.LastName, entity.BirthDate.Date);

        public static HashSet<Core.Models.Author> ToDomainModel(this HashSet<Models.JsonEntities.Author> entities)
            => new HashSet<Core.Models.Author>(entities.Select(x => x.ToDomainModel()));

        public static HashSet<Core.Models.Author> ToDomainModel (this HashSet<Models.XmlEntities.Author> entities)
            => new HashSet<Core.Models.Author>(entities.Select(x => x.ToDomainModel()));

        public static Models.JsonEntities.Author ToEntity(this Core.Models.Author domainModel)
            => new Models.JsonEntities.Author()
            {
                FirstName = domainModel.FirstName,
                LastName = domainModel.LastName,
                BirthDate = domainModel.BirthDate
            };

        public static Models.XmlEntities.Author ToXmlEntity(this Core.Models.Author domainModel)
            => new Models.XmlEntities.Author()
            {
                FirstName = domainModel.FirstName,
                LastName = domainModel.LastName,
                BirthDate = new DateOnlyXmlWrapper(domainModel.BirthDate)
            };

        public static HashSet<Models.JsonEntities.Author> ToEntity(this HashSet<Core.Models.Author> domainModels)
            => new HashSet<Models.JsonEntities.Author>(domainModels.Select(x => x.ToEntity()));

        public static HashSet<Models.XmlEntities.Author> ToXmlEntity(this HashSet<Core.Models.Author> domainModels)
            => new HashSet<Models.XmlEntities.Author>(domainModels.Select(x => x.ToXmlEntity()));
    }
}