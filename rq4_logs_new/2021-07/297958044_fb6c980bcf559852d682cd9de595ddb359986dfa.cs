using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using BehaviorDesigner.Runtime;
using BehaviorDesigner.Runtime.Tasks;

public class WaitLight : Action
{
    Car car;

    public override void OnStart()
    {
        car = gameObject.GetComponent<Car>();
        car.target = car.transform.position;

    }

    public override TaskStatus OnUpdate()
    {
        if(car.line.curLight != Line.Light.RED)
        {
            return TaskStatus.Success;
        }
        car.accel = 0;
        car.velocity = 0;
        return TaskStatus.Running;
    }

    public override void OnEnd()
    {
        base.OnEnd();
        car.line.cars.Remove(car.line.cars.Find(car));
    }
}