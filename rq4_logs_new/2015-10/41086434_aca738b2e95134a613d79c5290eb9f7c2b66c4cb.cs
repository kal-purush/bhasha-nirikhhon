namespace HO2.Domain.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class InitModel : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.Mate",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        Email = c.String(nullable: false, maxLength: 100),
                        FirstName = c.String(nullable: false, maxLength: 100),
                        LastName = c.String(nullable: false, maxLength: 100),
                    })
                .PrimaryKey(t => t.Id)
                .Index(t => t.Email, unique: true, name: "IX_UniqueEmail");
            
            CreateTable(
                "dbo.FriendGroup",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        FriendGroupName = c.String(nullable: false, maxLength: 100),
                        FriendGroupDetails = c.String(maxLength: 500),
                        FriendGroupAdminUser_Id = c.Int(),
                    })
                .PrimaryKey(t => t.Id)
                .ForeignKey("dbo.Mate", t => t.FriendGroupAdminUser_Id)
                .Index(t => t.FriendGroupAdminUser_Id);
            
            CreateTable(
                "dbo.Vote",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        UpdateDateTime = c.DateTime(nullable: false),
                        SuggestedDateTime = c.DateTime(nullable: false),
                        FriendGroup_Id = c.Int(nullable: false),
                        Mate_Id = c.Int(nullable: false),
                        Place_Id = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.Id)
                .ForeignKey("dbo.FriendGroup", t => t.FriendGroup_Id, cascadeDelete: true)
                .ForeignKey("dbo.Mate", t => t.Mate_Id, cascadeDelete: true)
                .ForeignKey("dbo.Place", t => t.Place_Id, cascadeDelete: true)
                .Index(t => t.FriendGroup_Id)
                .Index(t => t.Mate_Id)
                .Index(t => t.Place_Id);
            
            CreateTable(
                "dbo.Place",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        PlaceName = c.String(nullable: false, maxLength: 100),
                        Latitude = c.String(),
                        Longitude = c.String(),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.MateFriendGroup",
                c => new
                    {
                        FriendGroupId = c.Int(name: "FriendGroup.Id", nullable: false),
                        MateId = c.Int(name: "Mate.Id", nullable: false),
                    })
                .PrimaryKey(t => new { t.FriendGroupId, t.MateId })
                .ForeignKey("dbo.FriendGroup", t => t.FriendGroupId, cascadeDelete: true)
                .ForeignKey("dbo.Mate", t => t.MateId, cascadeDelete: true)
                .Index(t => t.FriendGroupId)
                .Index(t => t.MateId);
            
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.Vote", "Place_Id", "dbo.Place");
            DropForeignKey("dbo.Vote", "Mate_Id", "dbo.Mate");
            DropForeignKey("dbo.Vote", "FriendGroup_Id", "dbo.FriendGroup");
            DropForeignKey("dbo.MateFriendGroup", "Mate.Id", "dbo.Mate");
            DropForeignKey("dbo.MateFriendGroup", "FriendGroup.Id", "dbo.FriendGroup");
            DropForeignKey("dbo.FriendGroup", "FriendGroupAdminUser_Id", "dbo.Mate");
            DropIndex("dbo.MateFriendGroup", new[] { "Mate.Id" });
            DropIndex("dbo.MateFriendGroup", new[] { "FriendGroup.Id" });
            DropIndex("dbo.Vote", new[] { "Place_Id" });
            DropIndex("dbo.Vote", new[] { "Mate_Id" });
            DropIndex("dbo.Vote", new[] { "FriendGroup_Id" });
            DropIndex("dbo.FriendGroup", new[] { "FriendGroupAdminUser_Id" });
            DropIndex("dbo.Mate", "IX_UniqueEmail");
            DropTable("dbo.MateFriendGroup");
            DropTable("dbo.Place");
            DropTable("dbo.Vote");
            DropTable("dbo.FriendGroup");
            DropTable("dbo.Mate");
        }
    }
}