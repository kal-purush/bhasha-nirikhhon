using CommonDomainObjects;
using NHibernate;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Data
{
    public class ExclusivityLoader: IEtl<ClassificationScheme>
    {
        private readonly ICsvExtractor   _csvExtractor;
        private readonly ISessionFactory _sessionFactory;

        public ExclusivityLoader(
            ICsvExtractor   csvExtractor,
            ISessionFactory sessionFactory
            )
        {
            _csvExtractor   = csvExtractor;
            _sessionFactory = sessionFactory;
        }

        async Task<ClassificationScheme> IEtl<ClassificationScheme>.ExecuteAsync()
        {
            var classes = await _csvExtractor.ExtractAsync(
                "Exclusivity.csv",
                record => (
                    Class: new Class(
                        record[0] != null ? new Guid(record[0]) : Guid.NewGuid(),
                        record[2]),
                    SuperClassId: record[1] != null ? new Guid(record[1]) : Guid.Empty));

            var super = (
                from tuple in classes
                join superTuple in classes on tuple.SuperClassId equals superTuple.Class.Id into superTuples
                select
                (
                    Class: tuple.Class,
                    SuperClasses: (IList<Class>)superTuples.Select(t => t.Class).ToList()
                )).ToDictionary(
                    tuple => tuple.Class,
                    tuple => tuple.SuperClasses);

            var classificationScheme = new ClassificationScheme(super);
            using(var session = _sessionFactory.OpenSession())
            using(var transaction = session.BeginTransaction())
            {
                await session.SaveAsync(classificationScheme);
                await classificationScheme.VisitAsync(
                    async classificationSchemeClass =>
                    {
                        await session.SaveAsync(classificationSchemeClass.Class);
                        await session.SaveAsync(classificationSchemeClass);
                    },
                    null);
                await transaction.CommitAsync();
            }

            return classificationScheme;
        }
    }
}