using UnityEngine;
using UnityEditor;
using UnityEngine.TestTools;
using NUnit.Framework;
using System.Collections;
using System.Collections.Generic;

public class TestNodeDirection {

	[Test]
	public void Test_DirectionBinaryOperations_SanityCheckNone()
    {
        Assert.AreEqual(NODE_DIRECTION.NONE, NODE_DIRECTION.NONE);
	}

    [Test]
    public void Test_DirectionBinaryOperations_CheckNones()
    {
        Assert.True(NODE_DIRECTION.NONE.CheckFlag(NODE_DIRECTION.NONE));
    }

    [Test]
    public void Test_DirectionBinaryOperations_CheckNonNones()
    {
        Assert.True(NODE_DIRECTION.UP_DOWN_LEFT_RIGHT.CheckFlag(NODE_DIRECTION.UP_DOWN_LEFT_RIGHT));
    }

    [Test]
    public void Test_DirectionBinaryOperations_CheckInexact()
    {
        Assert.True(NODE_DIRECTION.UP_DOWN_LEFT_RIGHT.CheckFlag(NODE_DIRECTION.UP));
    }

    [Test]
    public void Test_DirectionBinaryOperations_CheckInexact_False()
    {
        Assert.False(NODE_DIRECTION.UP_DOWN.CheckFlag(NODE_DIRECTION.LEFT));
    }

    [Test]
    public void Test_PathEntrancesAndExits_Node()
    {
        Node n = new Node(NODE_DIRECTION.UP, NODE_DIRECTION.DOWN);
        Assert.AreEqual(NODE_DIRECTION.UP_DOWN, n.GetPathEntrancesAndExits());
    }

    [Test]
    public void Test_PathEntrancesAndExits_NodeList()
    {
        List<Node> nl = new List<Node>();
        nl.Add(new Node(NODE_DIRECTION.UP, NODE_DIRECTION.UP));
        nl.Add(new Node(NODE_DIRECTION.UP, NODE_DIRECTION.DOWN));
        nl.Add(new Node(NODE_DIRECTION.UP, NODE_DIRECTION.LEFT));
        Assert.AreEqual(NODE_DIRECTION.UP_DOWN_LEFT, nl.GetPathEntrancesAndExits());
    }
}