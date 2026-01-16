using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RotationAI : AI
{

    public override void pushChecker()
    {
        Debug.Log("RotationAI");
        keepObj = checkers[0].objTransform;
        for (int i = 1; i < checkers.Count; ++i)
            if (checkers[i].objTransform.position.y > keepObj.position.y)
                keepObj = checkers[i].objTransform;
        keepChecker = keepObj.GetComponent<Checker>();

        keepChecker.OnMouseDown();
        target = new Vector2(UnityEngine.Random.Range(-2.5f, 2.5f), upBorder - 1.2f * keepChecker.getRadius());

        StartCoroutine(waitRot());
    }

    /* Подсчет угла поворота
     * Входные параметры:
     * A - координаты позиции шайбы
     * B - координаты цели
     * Выходыне параметры:
     * Угол поворота в градусах
     */
    private float calculateAngle(Vector2 A, Vector2 B)
    {
        float X, Y; // по сути две стороны прямоугольного треугольника

        X = Math.Abs(A.x - B.x);
        Y = Math.Abs(A.y - B.y);

        float alpha = (float)Math.Atan(X / Y) * 57.3f;

        if (alpha > 45f)
            alpha = 45f;

        if (A.x < B.x)
            return -alpha;
        else
            return alpha;
    }

    IEnumerator waitRot()
    {
        while (statusType != Status.wait)
            yield return new WaitForSeconds(0.05f);
        
        keepChecker.angle -= calculateAngle(target, new Vector2(UnityEngine.Random.Range(leftBorder, rightBorder), 0));
        keepObj.rotation = Quaternion.Euler(0, 0, keepChecker.angle);
    }
}