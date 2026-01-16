import org.newdawn.slick.geom.Vector2f;

import java.util.ArrayList;

public class Physics
{


    static void ForceGravityOnAllUnits(ArrayList<? extends Unit> unit)
    {

        for ( Unit CurrentUnit : unit)
        {
            ///Если юнит в воздухе - сила тяжести  действует
            if (!CurrentUnit.isOnEarth()) {

                if (CurrentUnit.getListForce().containsKey("Gravity"))
                    CurrentUnit.addSpeed(CurrentUnit.getListForce().get("Gravity").getX() / 1000, CurrentUnit.getListForce().get("Gravity").getY() / 1000);
            }
            // иначе (когда юнит на земле)устанавливаем флаг состояния прыжка в false
            else CurrentUnit.setJump(false);
        }
    }
}