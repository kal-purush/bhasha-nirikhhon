using CommonDomainObjects;
using NHibernate;
using NHibernate.Criterion;
using System;
using System.Data;
using System.Threading.Tasks;

namespace Service
{
    public class ClassificationSchemeService: IDomainObjectService<Guid, ClassificationScheme>
    {
        protected readonly ISession _session;

        public ClassificationSchemeService(
            ISession session
            )
        {
            _session = session;
        }

        async Task<ClassificationScheme> IDomainObjectService<Guid, ClassificationScheme>.GetAsync(
            Guid id
            )
        {
            using(_session.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                _session
                    .CreateCriteria<ClassificationSchemeClass>()
                    .Fetch("Sub")
                    .Fetch("Class")
                    .CreateCriteria("ClassificationScheme")
                        .Add(Expression.Eq("Id", id))
                    .Future<ClassificationSchemeClass>();
                return await _session
                    .CreateCriteria<ClassificationScheme>()
                    .Add(Expression.Eq("Id", id))
                    .Fetch("Classes")
                    .FutureValue<ClassificationScheme>().GetValueAsync();
            }
        }
    }
}