using System.Collections;
using System.Collections.Generic;
using System.DirectoryServices;

namespace Affecto.ActiveDirectoryService
{
    internal class GroupPrincipal : Principal
    {
        public GroupPrincipal(DomainPath domainPath, DirectoryEntry directoryEntry, ICollection<string> additionalPropertyNames)
            : base(domainPath, directoryEntry, additionalPropertyNames)
        {
            ChildDomainPaths = GetChildDomainPaths(directoryEntry);
        }

        public override bool IsGroup
        {
            get { return true; }
        }

        public override bool IsActive
        {
            get { return true; }
        }

        internal IReadOnlyCollection<string> ChildDomainPaths { get; private set; }

        private static List<string> GetChildDomainPaths(DirectoryEntry directoryEntry)
        {
            var memberPaths = new List<string>();
            PropertyValueCollection memberValueCollection = directoryEntry.Properties[ActiveDirectoryProperties.Member];

            if (memberValueCollection != null)
            {
                IEnumerator memberEnumerator = memberValueCollection.GetEnumerator();
                while (memberEnumerator.MoveNext())
                {
                    if (memberEnumerator.Current != null)
                    {
                        memberPaths.Add(AdDomainPathHandler.Escape(memberEnumerator.Current.ToString()));
                    }
                }
            }

            return memberPaths;
        }
    }
}