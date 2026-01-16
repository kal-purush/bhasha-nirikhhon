namespace GoodQRma.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class fileChanges : DbMigration
    {
        public override void Up()
        {
            RenameColumn(table: "dbo.File", name: "User_Id", newName: "ApplicationUser_Id");
            RenameIndex(table: "dbo.File", name: "IX_User_Id", newName: "IX_ApplicationUser_Id");
            AddColumn("dbo.File", "EventId", c => c.Int(nullable: false));
            AlterColumn("dbo.Event", "userID", c => c.String());
            CreateIndex("dbo.File", "EventId");
            AddForeignKey("dbo.File", "EventId", "dbo.Event", "eventID", cascadeDelete: true);
            DropColumn("dbo.File", "userId");
        }
        
        public override void Down()
        {
            AddColumn("dbo.File", "userId", c => c.Int(nullable: false));
            DropForeignKey("dbo.File", "EventId", "dbo.Event");
            DropIndex("dbo.File", new[] { "EventId" });
            AlterColumn("dbo.Event", "userID", c => c.Int());
            DropColumn("dbo.File", "EventId");
            RenameIndex(table: "dbo.File", name: "IX_ApplicationUser_Id", newName: "IX_User_Id");
            RenameColumn(table: "dbo.File", name: "ApplicationUser_Id", newName: "User_Id");
        }
    }
}