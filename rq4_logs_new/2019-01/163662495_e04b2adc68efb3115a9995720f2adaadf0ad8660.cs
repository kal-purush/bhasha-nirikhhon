using CommunityNetwork.Common.Inerfaces;
using CommunityNetwork.Common.Models;
using CommunityNetWork.Common.Enums;

using System;
using System.Collections.Generic;


namespace CommunityNetWork.Dal.Interfaces
{
    public interface IGraph:IDBConnector
    {
        TNode Get<TNode>(Guid id) where TNode : INode;
        TNode Create<TNode>(TNode node) where TNode : INode;
        void Put(Node node);
        bool Delete<TNode>(Guid id) where TNode : INode;
        bool CreateAndLink<TNode, TLinked>(Guid nodeId, TLinked linked, Linkage linkage)
            where TNode : INode
            where TLinked : INode;
        bool IsLinked<TNode, TLinked>(Guid nodeId, Guid linkedId, Linkage linkage)
            where TNode : INode where TLinked : INode;
        bool IsLinkedBy<TNode, TLinkedBy>(Guid nodeId, Guid linkedId, Linkage linkage)
            where TNode : INode where TLinkedBy : INode;
        List<LinkedNode> GetLinksBy<TNode, TLinked>(Node linkedBy, Linkage linkage)
            where TNode : INode
            where TLinked : INode;
        
        bool CreateAndLinkWithParams<TNode>(Guid nodeId, TNode linked, Linkage linkage, LinkParams linkParams) where TNode : INode;
        bool Link<TNode, TLinked>(Guid nodeId, Guid linkedId, Linkage linkage) where TNode : Node where TLinked : INode;
        bool UnLink<TNode, TLinked>(Guid nodeId, Guid linkedId, Linkage linkage) 
            where TNode : INode where TLinked : INode;
        bool LinkWithParams<TNode, TLinked>(Guid nodeId, Guid linkedId, Linkage linkage, LinkParams linkParams) 
            where TNode : INode where TLinked : INode;
        List<TLinked> GetNodeLinks<TNode, TLinked>(Guid nodeId, Linkage linkage)
            where TNode : INode
            where TLinked : INode;
        List<TLinked> GetNodeNewLinks<TNode, TLinked>(Guid nodeId, Linkage linkage, DateTime dateTime) where TNode : INode;
        List<TLinkedBy> GetNodeLinksBy<TNode, TLinkedBy>(Guid nodeId, Linkage linkage) 
            where TNode : INode
            where TLinkedBy:INode;
        List<TLinkedByLinkedBy> GetNodeLinkedByLinkedBy<TNode, TLinkedBy, TLinkedByLinkedBy>(Guid nodeId, Linkage linkage, Linkage linkageBy) 
            where TNode : INode where TLinkedBy : INode where TLinkedByLinkedBy : INode;
        List<Tuple<TLinkedBy, TLinkedByLinkedBy>> GetNodeNewLinkedByLinkedBy<TNode, TLinkedBy, TLinkedByLinkedBy>(Guid nodeId, Linkage linkage, Linkage linkageBy, DateTime dateTime) where TNode : INode where TLinkedBy : INode where TLinkedByLinkedBy : INode;
        Dictionary<TNode, List<TLinked>> GetNodesLinks<TNode, TLinked>(Linkage linkage)
        where TNode : INode where TLinked : INode;
        List<Tuple<TNode, List<TLinkedBy>>> GetNodesLinksBy<TNode, TLinkedBy>(Guid nodeId,
                                                                                Linkage linkage)
        where TNode : INode where TLinkedBy : INode;


    }
}