namespace Web2ProjekatBackend.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class NovaMigraCigra : DbMigration
    {
        public override void Up()
        {
            DropForeignKey("dbo.OpremaIncidents", "Oprema_Id", "dbo.Opremas");
            DropIndex("dbo.OpremaIncidents", new[] { "Oprema_Id" });
            RenameColumn(table: "dbo.OpremaIncidents", name: "Oprema_Id", newName: "Oprema_IdOprema");
            DropPrimaryKey("dbo.Opremas");
            DropPrimaryKey("dbo.OpremaIncidents");
            AddColumn("dbo.Opremas", "IdOprema", c => c.String(nullable: false, maxLength: 128));
            AlterColumn("dbo.Opremas", "Name", c => c.String(nullable: false, maxLength: 255));
            AlterColumn("dbo.Opremas", "OpremaType", c => c.String(nullable: false, maxLength: 255));
            AlterColumn("dbo.Opremas", "Coordinates", c => c.String(nullable: false, maxLength: 255));
            AlterColumn("dbo.Opremas", "Address", c => c.String(nullable: false, maxLength: 255));
            AlterColumn("dbo.OpremaIncidents", "Oprema_IdOprema", c => c.String(nullable: false, maxLength: 128));
            AddPrimaryKey("dbo.Opremas", "IdOprema");
            AddPrimaryKey("dbo.OpremaIncidents", new[] { "Oprema_IdOprema", "Incident_ID" });
            CreateIndex("dbo.OpremaIncidents", "Oprema_IdOprema");
            AddForeignKey("dbo.OpremaIncidents", "Oprema_IdOprema", "dbo.Opremas", "IdOprema", cascadeDelete: true);
            DropColumn("dbo.Opremas", "Id");
        }
        
        public override void Down()
        {
            AddColumn("dbo.Opremas", "Id", c => c.Int(nullable: false, identity: true));
            DropForeignKey("dbo.OpremaIncidents", "Oprema_IdOprema", "dbo.Opremas");
            DropIndex("dbo.OpremaIncidents", new[] { "Oprema_IdOprema" });
            DropPrimaryKey("dbo.OpremaIncidents");
            DropPrimaryKey("dbo.Opremas");
            AlterColumn("dbo.OpremaIncidents", "Oprema_IdOprema", c => c.Int(nullable: false));
            AlterColumn("dbo.Opremas", "Address", c => c.String());
            AlterColumn("dbo.Opremas", "Coordinates", c => c.String());
            AlterColumn("dbo.Opremas", "OpremaType", c => c.String());
            AlterColumn("dbo.Opremas", "Name", c => c.String());
            DropColumn("dbo.Opremas", "IdOprema");
            AddPrimaryKey("dbo.OpremaIncidents", new[] { "Oprema_Id", "Incident_ID" });
            AddPrimaryKey("dbo.Opremas", "Id");
            RenameColumn(table: "dbo.OpremaIncidents", name: "Oprema_IdOprema", newName: "Oprema_Id");
            CreateIndex("dbo.OpremaIncidents", "Oprema_Id");
            AddForeignKey("dbo.OpremaIncidents", "Oprema_Id", "dbo.Opremas", "Id", cascadeDelete: true);
        }
    }
}