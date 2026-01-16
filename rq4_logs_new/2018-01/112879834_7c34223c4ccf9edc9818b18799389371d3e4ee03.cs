using UnityEngine;
using UnityEngine.TestTools;
using NUnit.Framework;
using System.Collections;
using UnityEngine.SceneManagement;

public class TestMinionMovement
{

	// A UnityTest behaves like a coroutine in PlayMode
	// and allows you to yield null to skip a frame in EditMode
	[UnityTest]
	public IEnumerator TestMinionMovementWithEnumeratorPasses() {
        SceneManager.LoadScene("Test_MinionMovement");

		yield return null;
        Node origin = new Node(0, 0);
        Path path = new Path(origin, NODE_DIRECTION.RIGHT); //1

        Branch testBranch = path.Branches[path.OuterBranchIDs[0]];
        testBranch.Add(NODE_DIRECTION.RIGHT); //2
        testBranch.Add(NODE_DIRECTION.RIGHT); //3
        testBranch.Add(NODE_DIRECTION.RIGHT); //4

        Node startNode = testBranch.InNode;
        Assert.That(startNode.VectorLocation, Is.EqualTo(new Vector2Int(4, 0)));

        MovementController minion = GameObject.Instantiate<MovementController>(Resources.Load<MovementController>("prefabs/enemy/enemy"));
        minion.Activate(startNode);

        float travelDuration = minion.MoveInterval * Node.GetDistanceToOrigin(startNode) + 1f;

        yield return new WaitForSeconds(5);
        Assert.That(minion.transform.position.RoundToVector2Int(), Is.EqualTo(origin.VectorLocation));
	}
}