using UnityEngine;
using UnityEditor;
using UnityEngine.TestTools;
using NUnit.Framework;
using System.Collections;

public class TestBranch {
    [Test]
    public void Test_Branch_Constructor()
    {
        Node o = new Node(2, 2);
        Branch b = new Branch(1, o, NODE_DIRECTION.UP);

        Assert.That(b.OutNode.VectorLocation, Is.EqualTo(o.VectorLocation));
    }

    [Test]
	public void Test_BranchAdd_Single()
    {
        Node o = new Node(2, 2);
        Branch_Test b = new Branch_Test(1, o, NODE_DIRECTION.UP);

        b.Add(NODE_DIRECTION.UP);
        
        Vector2Int ex = new Vector2Int(2, 4);
        Vector2Int act = b.currentNode.VectorLocation;

        Assert.That(act, Is.EqualTo(ex));
	}

    [Test]
    public void Test_BranchAdd_Multi_Closed()
    {
        Node o = new Node(2, 2);
        Branch b = new Branch(1, o, NODE_DIRECTION.UP);

        b.Add(NODE_DIRECTION.UP_LEFT_RIGHT);

        Assert.That(!b.Open);
    }

    [Test]
    public void Test_BranchAdd_Multi_InNode()
    {
        Node o = new Node(2, 2);
        Branch b = new Branch(1, o, NODE_DIRECTION.UP);

        b.Add(NODE_DIRECTION.UP_LEFT_RIGHT);
        Vector2Int ex = new Vector2Int(2, 3);

        Assert.That(b.InNode.VectorLocation, Is.EqualTo(ex));
    }

    [Test]
    public void Test_BranchAdd_Multi_Return()
    {
        Node o = new Node(2, 2);
        Branch b = new Branch(1, o, NODE_DIRECTION.UP);

        NODE_DIRECTION[] act = b.Add(NODE_DIRECTION.UP_LEFT_RIGHT);

        NODE_DIRECTION[] ex = { NODE_DIRECTION.UP, NODE_DIRECTION.LEFT, NODE_DIRECTION.RIGHT };

        Assert.That(act, Is.EqualTo(ex));
    }

    [Test]
    public void Test_BranchAdd_Closed()
    {
        Node o = new Node(2, 2);
        Branch b = new Branch(1, o, NODE_DIRECTION.UP);

        b.Add(NODE_DIRECTION.UP_LEFT_RIGHT);

        Assert.Throws<BranchException>(() => b.Add(NODE_DIRECTION.UP));
    }
}