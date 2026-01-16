using CommunityNetwork.Common.Inerfaces;
using CommunityNetwork.Common.Models;
using CommunityNetWork.Common.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommunityNetWork.Dal
{
    public static class Neo4JFormat
    {
        public const string LinkerTag = "linker";
        public const string LinkedByTag = "linkedBy";
        public const string LinkedByLinkedByTag = "linkedByLinkedBy";
        const string MatchDefinedLabelFormat = "(node:{0})";
        const string CreateFormat = "(node:{0}{{newNode}})";
        const string CreateAndLinkFormat = "(linker:{0})-[:{1}]->(linkedBy:{2}{{newNode}})";

       const string MatchDeleteLabelFormat = "(unlinkednode:{0}),(linkednode:{0})-[r]-()";

        const string MatchDetachFormat = "(node:{0})";

         const string MatchAnyDefinedLinkageFormat = "(node1:{0})-[r:{1}]-(node2:{2})";
         const string MatchAnyUndefinedLinkageFormat = "(node1)-[:{0}]-(node2)";

         const string MatchFullDefinedLinkageFormat = "(linkedBy:{0})<-[r:{1}]-(linker:{2})";
         const string MatchFullUndefinedLinkageFormat = "(linkedBy:{0})<-[:{1}]-(linker:{2})";

         const string MatchLinkedByUndefinedLinkageFormat = "(linkedBy:{0})<-[:{1}]-(:{2})";

        const string MatchLinkedByLinkedByUndefinedLinkageFormat = "(linkedByLinkedBy:{0})<-[:{1}]-(:{2})";

        const string MatchFullDefinedLinkageByLinkageFormat = "(linker:{0})-[r1:{1}]->(linkedBy:{2})-[r2:{3}]->(linkedByLinkedBy:{4})";

        const string MatchFullUndefinedLinkageByLinkageFormat = "(linker:{0})-[:{1}]->(linkedBy:{2})-[:{3}]->(linkedByLinkedBy:{4})";



        const string LinkageFormat = "(linker)-[:{0}]->(linkedBy)";
        const string DefinedLinkageFormat = "(linker)-[r:{0}]->(linkedBy)";

        static readonly string ByLinkageWithParamsFormat = "(linker)-[:{0} {{relParam}}]->(linkedBy)";
        static readonly string LinkageWithParamsFormat = "(linkedBy)<-[:{0} {{relParam}}]-(linker)";

        const string CreateAndLinkWithParamsFormat = "(linker:{0})-[:{0}{{relParam}}]->(linkedBy:{1}{{newNode}})";

        public static string TypeName<T>()
        {
            return typeof(T).Name;

        }

        public static string GetMatchLabelFormat(int counter)
        {
            string labelFormat = "";
            if (counter > 0)
                labelFormat += ",";
            labelFormat += "node" + (counter + 1) + ":{" + counter + "})";
            return labelFormat;
        }

        public static string GetMatchLabelFormat(int counter, string tag)
        {
            string labelFormat = "";
            if (counter > 0)
                labelFormat += ",";
            labelFormat += "(" + tag + ":{" + counter + "})";
            return labelFormat;
        }
        public static string GetMatchLabelString<TNode>(string tag)
        {
            string format = "({0}:{1})";
            return string.Format(format, tag, TypeName<TNode>());
        }


        public static string GetMatchDefinedLabelString<TNode>()
        {

            string format = MatchDefinedLabelFormat;
            return string.Format(format, TypeName<TNode>());
        }


        public static string GetMatchLabelsString(params MNode[] nodes)
        {
            string[] nodesLabels = new string[nodes.Length];
            string format = "";//"(";
            for (int i = 0; i < nodes.Length; i++)
            {
                format += GetMatchLabelFormat(i);
                nodesLabels[i] = nodes[i].NodeName;
            }
            return string.Format(format, nodesLabels);
        }


        public static string GetMatchLabelsString<TNode>(params string[] nodesTags)
            where TNode : INode
        {
            string[] nodesLabels = new string[nodesTags.Length];
            string format = "";
            for (int i = 0; i < nodesTags.Length; i++)
            {
                format += GetMatchLabelFormat(i, nodesTags[i]);
                nodesLabels[i] = TypeName<TNode>();
            }

            return string.Format(format, nodesLabels);
        }

        
        public static string GetMatchLinkedByUndefinedLinkageString<TLinkedBy, TLinker>(Linkage linkage)
        {
            return string.Format(MatchLinkedByUndefinedLinkageFormat, TypeName<TLinkedBy>(), linkage, TypeName<TLinker>());
        }


        public static string GetMatchLinkedByLinkedByUndefinedLinkageString<TLinkedByLinkedBy, TLinkedBy>(Linkage linkage)
        {
            return string.Format(MatchLinkedByLinkedByUndefinedLinkageFormat, TypeName<TLinkedByLinkedBy>(), linkage, TypeName<TLinkedBy>());
        }


        public static string GetMatchFullDefinedLinkageString<TLinkedBy, TLinker>(Linkage linkage)
        {
            return string.Format(MatchFullDefinedLinkageFormat, TypeName<TLinkedBy>(), linkage, TypeName<TLinker>());
        }
        public static string GetMatchFullUndefinedLinkageString<TLinkedBy, TLinker>(Linkage linkage)
        {
            return string.Format(MatchFullUndefinedLinkageFormat, TypeName<TLinkedBy>(), linkage, TypeName<TLinker>());
        }

        public static string GetMatchByLinkageByLinkageString<TLinkedByLinkedBy, TLinkedBy, TLinker>(Linkage linkage, Linkage linkageBy)
        {
            return string.Format(MatchFullDefinedLinkageByLinkageFormat, TypeName<TLinker>(), linkage, TypeName<TLinkedBy>(), linkageBy, TypeName<TLinkedByLinkedBy>());
        }

        public static string GetNoLinkageString(Linkage linkage)
        {
            return "not " + GetMatchAnyLinkageString(linkage);
        }

        public static string GetCreateString<TNode>()
        {
            return string.Format(CreateFormat, TypeName<TNode>());
        }


        public static string GetLinkageString(Linkage linkage)
        {
            return string.Format(LinkageFormat, linkage);
        }

        public static string GetDefinedLinkageString(Linkage linkage)
        {
            return string.Format(DefinedLinkageFormat, linkage);
        }
        public static string GetMatchDeleteLabelString<TNode>()
        {
            return string.Format(MatchDeleteLabelFormat, TypeName<TNode>());
        }


        public static string GetMatchDetachString<TNode>()
        {
            return string.Format(MatchDetachFormat, TypeName<TNode>());
        }

        public static string GetMatchOneDefinedLinkageString<TNode, TLinked>(Linkage linkage)
        {
            return string.Format(MatchFullDefinedLinkageFormat, TypeName<TNode>(), linkage, TypeName<TLinked>());
        }


        public static string GetMatchAnyDefinedLinkageString<TNode, TLinked>(Linkage linkage)
        {
            return string.Format(MatchAnyDefinedLinkageFormat, TypeName<TNode>(), linkage, TypeName<TLinked>());
        }

        public static string GetMatchAnyLinkageString<TNode, TLinked>(Linkage linkage)
        {
            return string.Format(MatchAnyDefinedLinkageFormat, TypeName<TNode>(), linkage, TypeName<TLinked>());
        }

        public static string GetMatchAnyLinkageString(Linkage linkage)
        {
            return string.Format(MatchAnyUndefinedLinkageFormat, linkage);
        }


        public static string GetMatchDefinedLinkageString<TNode, TLinked>(Linkage linkage)
        {
            return string.Format(MatchFullDefinedLinkageFormat, TypeName<TNode>(), linkage, TypeName<TLinked>());
        }

        public static string GetLinkageWithParamsString(Linkage linkage)
        {
            return string.Format(LinkageWithParamsFormat, linkage);

        }
        public static string GetByLinkageWithParamsString(Linkage linkage)
        {
            return string.Format(ByLinkageWithParamsFormat, linkage);

        }
        

        public static string GetCreateAndLinkString<TLinked, TNew>(Linkage linkage)
        {
            return string.Format(CreateAndLinkFormat, TypeName<TLinked>(), linkage, TypeName<TNew>());
        }

        public static string GetCreateAndLinkWithParamsString<TLinked, TNew>(Linkage linkage)
        {
            return string.Format(CreateAndLinkWithParamsFormat, TypeName<TLinked>(), linkage, TypeName<TNew>());
        }

    }
}