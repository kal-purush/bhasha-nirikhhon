namespace GolfApp.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class Initial : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.Game",
                c => new
                    {
                        GameId = c.Int(nullable: false, identity: true),
                        UserId_UserId = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.GameId)
                .ForeignKey("dbo.Users", t => t.UserId_UserId, cascadeDelete: true)
                .Index(t => t.UserId_UserId);
            
            CreateTable(
                "dbo.Users",
                c => new
                    {
                        UserId = c.Int(nullable: false, identity: true),
                        Username = c.String(nullable: false),
                        Password = c.String(nullable: false),
                    })
                .PrimaryKey(t => t.UserId);
            
            CreateTable(
                "dbo.Holes",
                c => new
                    {
                        HoleId = c.Int(nullable: false, identity: true),
                        TotalShots = c.Int(nullable: false),
                        DriveType = c.String(),
                        OnFairway = c.Boolean(nullable: false),
                        InRegulation = c.Boolean(nullable: false),
                        Par = c.Int(nullable: false),
                        NumPutts = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.HoleId);
            
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.Game", "UserId_UserId", "dbo.Users");
            DropIndex("dbo.Game", new[] { "UserId_UserId" });
            DropTable("dbo.Holes");
            DropTable("dbo.Users");
            DropTable("dbo.Game");
        }
    }
}