using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PatternsPractice.Creational
{
    public static class AbstractConstant
    {
        public const string MESSAGE = "Nightmare after watching {0} ? We will end this day with {1}.";
    }

    class AbstractFactoryPattern
    {
        //public static void Main(string[] args)
        //{
        //    Client client = new Client(new SaturdayShow());
        //    Console.WriteLine("***** " + client.factory.GetType() + " *****");
        //    client.Interact();
        //    client = new Client(new SundayShow());
        //    Console.WriteLine("***** " + client.factory.GetType() + " *****");
        //    client.Interact();
        //    Console.ReadKey();
        //}
    }

    #region Horror Abstraction

    abstract class AbstractHorror
    {
        public abstract string AirHorror();
        public abstract void FinalShow(AbstractComedy comedy);
    }

    class HorrorMovie : AbstractHorror
    {
        public override string AirHorror()
        {
            return "SAW";
        }

        public override void FinalShow(AbstractComedy comedy)
        {
            Console.WriteLine(AbstractConstant.MESSAGE, this.AirHorror(), comedy.AirComedy());
        }
    }

    class HorrorSeries : AbstractHorror
    {
        public override string AirHorror()
        {
            return "THE WALKING DEAD";
        }

        public override void FinalShow(AbstractComedy comedy)
        {
            Console.WriteLine(AbstractConstant.MESSAGE, this.AirHorror(), comedy.AirComedy());
        }
    }

    #endregion

    #region Comedy Abstraction

    abstract class AbstractComedy
    {
        public abstract string AirComedy();
    }

    class ComedyMovie : AbstractComedy
    {
        public override string AirComedy()
        {
            return "THIS IS THE END";
        }
    }

    class ComedySeries : AbstractComedy
    {
        public override string AirComedy()
        {
            return "FRIENDS";
        }
    }

    #endregion

    abstract class AbstractFactory
    {
        public abstract AbstractHorror GetHorror();
        public abstract AbstractComedy GetComedy();
    }


    class SaturdayShow : AbstractFactory
    {
        public override AbstractHorror GetHorror()
        {
            return new HorrorMovie();
        }

        public override AbstractComedy GetComedy()
        {
            return new ComedySeries();
        }
    }

    class SundayShow : AbstractFactory
    {
        public override AbstractHorror GetHorror()
        {
            return new HorrorSeries();
        }

        public override AbstractComedy GetComedy()
        {
            return new ComedyMovie();
        }
    }

    class Client
    {
        AbstractHorror _horror;
        AbstractComedy _comedy;
        public AbstractFactory factory;

        public Client(AbstractFactory factory)
        {
            this.factory = factory;
            _horror = factory.GetHorror();
            _comedy = factory.GetComedy();
        }

        public void Interact()
        {
            _horror.FinalShow(_comedy);
        }
    }

}