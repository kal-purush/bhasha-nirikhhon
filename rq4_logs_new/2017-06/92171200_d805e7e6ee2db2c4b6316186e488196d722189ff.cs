namespace eDnevnikDev.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class PopunjavanjeSmerova : DbMigration
    {
        public override void Up()
        {
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Veterinarski tehničar', 4)");
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Poljoprivredni tehničar', 4)");        
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Prehrambeni tehničar', 4)");
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Tehničar hortikulture', 4)");
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Mesar', 3)");
            Sql("INSERT INTO Smer(NazivSmera, Trajanje) VALUES(N'Rukovalac-mehaničar privredne tehnike', 3)");

        }

        public override void Down()
        {
            Sql("DELETE FROM Smer");

        }
    }
}