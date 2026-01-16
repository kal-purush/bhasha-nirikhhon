using System.Linq;
using Catalog.Domain.Entity;
using XRoad.Domain;

namespace Catalog.DataAccessLayer.Helpers
{
    public static class MembersDbSetHelpers
    {
        public static Member FindByMemberIdentifier(this IQueryable<Member> source,
            MemberIdentifier memberIdentifier)
        {
            return source.SingleOrDefault(
                it => it.Instance == memberIdentifier.Instance
                    && it.MemberClass == memberIdentifier.MemberClass
                    && it.MemberCode == memberIdentifier.MemberCode);
        }
    }
}