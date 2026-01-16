using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using NUnit.Framework;

[TestFixture]
public class NodeTest  {

	[Test]
	public void TestValidPosition()
	{
		GameObject nodeObj = new GameObject ("Node");
		nodeObj.AddComponent<MyNode>();
		MyNode node = nodeObj.GetComponent<MyNode> ();
		//MyNode node = new MyNode();
		GameObject g = new GameObject ("empty");
		node.transform.SetParent (g.transform);

		g.name = "Left_1";
		Assert.IsFalse(node.validPosition());

		g.name = "Left_2";
		Assert.IsFalse(node.validPosition());

		g.name = "Right_1";
		Assert.IsFalse(node.validPosition());

		g.name = "Right_2";
		Assert.IsFalse(node.validPosition());

		g.name = "Root";
		Assert.IsTrue(node.validPosition());
	}
}