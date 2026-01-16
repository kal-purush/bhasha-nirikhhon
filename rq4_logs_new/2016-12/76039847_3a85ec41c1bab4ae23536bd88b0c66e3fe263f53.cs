using UnityEngine;
using UnityEditor;
using NUnit.Framework;

public class MoveTest {

    [Test]
    public void MoveTransformToTest() {
        //given
        var move = new Move();
        var trans = new GameObject().GetComponent<Transform>();
        trans.position = Vector3.zero;
        var dest = new Vector2(10f, 0f);

        //when
        move.MoveTransformTo(trans, dest);

        //then
        Assert.AreEqual(new Vector3(10f, 0f, 0f), trans.position);
    }

    [Test]
    public void FixPositionWithMovableAreaTest() {
        //given
        var move = new Move();
        var leftBottom = new Vector2(-5f, -5f);
        var rightTop = new Vector2(5f, 5f);
        move.SetMovableArea(leftBottom, rightTop);
        var trans = new GameObject().GetComponent<Transform>();
        trans.position = Vector3.zero;
        var dest = new Vector2(10f, 0f);

        //when
        move.MoveTransformTo(trans, dest);

        //then
        Assert.AreEqual(new Vector3(5f, 0f, 0f), trans.position);
    }
}