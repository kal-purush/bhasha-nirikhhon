using System;


namespace comandos
{
    internal class ArmyCommando: Commando
    {
        string UnitName; 

        public ArmyCommando(string name, string codeName, string status, string unitName):base(name, codeName, status)
        {
            this.UnitName = unitName;
        }

        public override void Attack()
        {
            Console.WriteLine($"the Army Commando {CodeName} is attack");
        }
    }
}