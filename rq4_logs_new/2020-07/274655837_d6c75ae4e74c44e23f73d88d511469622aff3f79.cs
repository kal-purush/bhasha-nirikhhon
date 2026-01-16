using BaseStructures;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Rotation : Modifier
{
    Checker checker;

    private void Start()
    {
        checker = gameObject.GetComponent<Checker>();
    }

    void Update()
    {
        if (checker.getMouseDown())
        {
           
            Vector2 mousePos = Camera.main.ScreenToWorldPoint(Input.mousePosition);

            if (mousePos.y < checker.playerDownBorder.Down)
            {
                checker.setStop(true);
                checker.angle = 90 + calculateRotation(mousePos, checker.transform.position);
                checker.transform.rotation = Quaternion.Euler(0, 0, checker.angle);
               
            }
            else if (mousePos.y > checker.playerUpBorder.Up)
            {
                checker.setStop(true);
                checker.angle = 270 - calculateRotation(mousePos, checker.transform.position);
                checker.transform.rotation = Quaternion.Euler(0, 0, checker.angle);
            }
            else
                checker.setStop(false);
        }
    }

    /* Функция для вычисления угла поворота шайбы
     * Входные параметры:
     * A - координаты позиции пальца
     * B - координаты позиции шайбы
     * Выходные параметры:
     * Угол поворота, рассчет ведется относительно нижней части поля, для верхней необходимо поменять знак 
     */
    float calculateRotation(Vector2 A, Vector2 B)
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
}