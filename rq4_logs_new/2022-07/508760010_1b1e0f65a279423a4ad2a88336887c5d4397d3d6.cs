using cse210_10.Game.Casting;
namespace cse210_10.Game.Scripting
{
    public class MoveAliensAction : Action
    {
        private int countdown = 0;
        private int xSteps = 8;
        public MoveAliensAction()
        {
        }

        public void Execute(Cast cast, Script script, ActionCallback callback)
        {
            List<Actor> aliens = cast.GetActors(Constants.BRICK_GROUP);
            if (countdown <= 0) 
            {
                countdown = 30;
                xSteps -= 1;
                foreach (Actor actor in aliens) 
                {
                    Alien alien = (Alien)actor;
                    Body body = alien.GetBody();
                    Point position = body.GetPosition();
                    Point velocity = body.GetVelocity();
                    position = position.Add(velocity);
                    body.SetPosition(position);
                    if (xSteps <= 0) {
                        alien.BounceX();
                    }
                }
                if (xSteps < 0) {
                    xSteps = 8;
                }
            }
            else {
                countdown -= 1;
            }

        }
    }
}