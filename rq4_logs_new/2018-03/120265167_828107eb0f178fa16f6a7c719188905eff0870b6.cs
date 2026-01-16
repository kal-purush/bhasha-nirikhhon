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
                        currHole = c.Int(),
                        Hole1_HoleId = c.Int(),
                        Hole10_HoleId = c.Int(),
                        Hole11_HoleId = c.Int(),
                        Hole12_HoleId = c.Int(),
                        Hole13_HoleId = c.Int(),
                        Hole14_HoleId = c.Int(),
                        Hole15_HoleId = c.Int(),
                        Hole16_HoleId = c.Int(),
                        Hole17_HoleId = c.Int(),
                        Hole18_HoleId = c.Int(),
                        Hole2_HoleId = c.Int(),
                        Hole3_HoleId = c.Int(),
                        Hole4_HoleId = c.Int(),
                        Hole5_HoleId = c.Int(),
                        Hole6_HoleId = c.Int(),
                        Hole7_HoleId = c.Int(),
                        Hole8_HoleId = c.Int(),
                        Hole9_HoleId = c.Int(),
                        UserId_UserId = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.GameId)
                .ForeignKey("dbo.Holes", t => t.Hole1_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole10_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole11_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole12_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole13_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole14_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole15_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole16_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole17_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole18_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole2_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole3_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole4_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole5_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole6_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole7_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole8_HoleId)
                .ForeignKey("dbo.Holes", t => t.Hole9_HoleId)
                .ForeignKey("dbo.Users", t => t.UserId_UserId, cascadeDelete: true)
                .Index(t => t.Hole1_HoleId)
                .Index(t => t.Hole10_HoleId)
                .Index(t => t.Hole11_HoleId)
                .Index(t => t.Hole12_HoleId)
                .Index(t => t.Hole13_HoleId)
                .Index(t => t.Hole14_HoleId)
                .Index(t => t.Hole15_HoleId)
                .Index(t => t.Hole16_HoleId)
                .Index(t => t.Hole17_HoleId)
                .Index(t => t.Hole18_HoleId)
                .Index(t => t.Hole2_HoleId)
                .Index(t => t.Hole3_HoleId)
                .Index(t => t.Hole4_HoleId)
                .Index(t => t.Hole5_HoleId)
                .Index(t => t.Hole6_HoleId)
                .Index(t => t.Hole7_HoleId)
                .Index(t => t.Hole8_HoleId)
                .Index(t => t.Hole9_HoleId)
                .Index(t => t.UserId_UserId);
            
            CreateTable(
                "dbo.Holes",
                c => new
                    {
                        HoleId = c.Int(nullable: false, identity: true),
                        HoleNum = c.Int(nullable: false),
                        TotalShots = c.Int(nullable: false),
                        DriveType = c.String(),
                        OnFairway = c.Boolean(nullable: false),
                        InRegulation = c.Boolean(nullable: false),
                        Par = c.Int(nullable: false),
                        NumPutts = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.HoleId);
            
            CreateTable(
                "dbo.Users",
                c => new
                    {
                        UserId = c.Int(nullable: false, identity: true),
                        Username = c.String(nullable: false),
                        Password = c.String(nullable: false),
                    })
                .PrimaryKey(t => t.UserId);
            
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.Game", "UserId_UserId", "dbo.Users");
            DropForeignKey("dbo.Game", "Hole9_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole8_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole7_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole6_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole5_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole4_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole3_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole2_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole18_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole17_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole16_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole15_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole14_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole13_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole12_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole11_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole10_HoleId", "dbo.Holes");
            DropForeignKey("dbo.Game", "Hole1_HoleId", "dbo.Holes");
            DropIndex("dbo.Game", new[] { "UserId_UserId" });
            DropIndex("dbo.Game", new[] { "Hole9_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole8_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole7_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole6_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole5_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole4_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole3_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole2_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole18_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole17_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole16_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole15_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole14_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole13_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole12_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole11_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole10_HoleId" });
            DropIndex("dbo.Game", new[] { "Hole1_HoleId" });
            DropTable("dbo.Users");
            DropTable("dbo.Holes");
            DropTable("dbo.Game");
        }
    }
}