using System;
using System.Collections.Generic;
using CommunityNetwork.Common;
using CommunityNetwork.Common.Models;
using CommunityNetWork.Common.Enums;
using CommunityNetWork.Dal;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Social.BL.Models;

namespace BL.Test
{
    class NodeA : MNode
    {
        public int IntProperty { get; set; }
        public string StringProperty { get; set; }
        public int Follows { get; set; }
        public override bool Equals(object obj)
        {
            if (!(obj is NodeA))
                return false;
            NodeA n = (NodeA)obj;
            return Id.Equals(n.Id);
        }
        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }

    [TestClass]
    public class CommunicationTest
    {
        NodeA nA1, nA2, nA3, nA4;
        
        public void Init()
        {
            
            
            nA1 = new NodeA
            {
                Id = Guid.NewGuid().ToString(),
                IntProperty = 1,
                StringProperty = "1"


            };
            nA2 = new NodeA
            {
                Id = Guid.NewGuid().ToString(),
                IntProperty = 2,
                StringProperty = "2"


            };
            nA3 = new NodeA
            {
                Id = Guid.NewGuid().ToString(),
                IntProperty = 3,
                StringProperty = "3"


            };
            nA4 = new NodeA
            {
                Id = Guid.NewGuid().ToString(),
                IntProperty = 4,
                StringProperty = "3"


            };
        }

        [TestMethod]
        public void GetFollowersReturnA1AndA2()
        {
            List<NodeA> list;
            Init();
            using (Neo4jConnector neo4j = new Neo4jConnector())
            {
                neo4j.Delete<NodeA>();
                neo4j.Create(nA1);
                neo4j.Create(nA2);
                neo4j.Create(nA3);
            }
            Communication com = new Communication(new CommunityNetWork.Dal.Neo4JConnectorFactory());
            com.Link<NodeA, NodeA>(nA3.Id, nA1.Id, Linkage.Follow);
            com.Link<NodeA, NodeA>(nA3.Id, nA2.Id, Linkage.Follow);
                
            list = com.GetLinkers<NodeA, NodeA>(nA3.Id, Linkage.Follow);
            
            bool result = list.Count==2 
                          && list.Contains(nA1) 
                          && list.Contains(nA2);
            Assert.AreEqual(result, true);
        }

        [TestMethod]
        public void TestUnFollow()
        {
            List<NodeA> list1,list2;
            Init();
            using (Neo4jConnector neo4j = new Neo4jConnector())
            {
                neo4j.Delete<NodeA>();
                neo4j.Create(nA1);
                neo4j.Create(nA2);
                neo4j.Create(nA3);
            }
            Communication com = new Communication(new CommunityNetWork.Dal.Neo4JConnectorFactory());
            com.Link<NodeA, NodeA>(nA3.Id, nA1.Id, Linkage.Follow);
            com.Link<NodeA, NodeA>(nA3.Id, nA2.Id, Linkage.Follow);

            list1 = com.GetLinkers<NodeA, NodeA>(nA3.Id, Linkage.Follow);
            com.UnLink<NodeA, NodeA>(nA3.Id, nA1.Id, Linkage.Follow);
            list2= com.GetLinkers<NodeA, NodeA>(nA3.Id, Linkage.Follow);

            bool result = list1.Count == 2
                          && list1.Contains(nA1)
                          && list1.Contains(nA2)
                          && list2.Count == 1
                          && list1.Contains(nA2);
            Assert.AreEqual(result, true);
        }

        [TestMethod]
        public void GetFollowedReturnA1AndA2()
        {
            List<NodeA> list;
            Init();
            using (Neo4jConnector neo4j = new Neo4jConnector())
            {
                neo4j.Delete<NodeA>();
                neo4j.Create(nA1);
                neo4j.Create(nA2);
                neo4j.Create(nA3);
            }

            Communication com = new Communication(new CommunityNetWork.Dal.Neo4JConnectorFactory());
            com.Link<NodeA, NodeA>(nA1.Id, nA3.Id, Linkage.Follow);
            com.Link<NodeA, NodeA>(nA2.Id, nA3.Id, Linkage.Follow);
                
            list = com.GetLinkedBy<NodeA, NodeA>(nA3.Id, Linkage.Follow);
            
            bool result = list.Count==2 
                          && list.Contains(nA1) 
                          && list.Contains(nA2);
            Assert.AreEqual(result, true);
        }
    }
}