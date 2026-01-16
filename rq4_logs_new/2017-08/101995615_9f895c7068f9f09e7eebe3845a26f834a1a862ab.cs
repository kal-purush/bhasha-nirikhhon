namespace Fever.Compoments.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class Setup_AdminPlat_Core_database : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.CommandActionInfoModels",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        ActionName = c.String(maxLength: 50),
                        ControllerName = c.String(maxLength: 50),
                        CommandText = c.String(maxLength: 50),
                        CreateTime = c.DateTime(nullable: false),
                        UpdateDateTime = c.DateTime(nullable: false),
                        CreateBy = c.String(),
                        UpdateBy = c.String(),
                        IsSoftDelete = c.Boolean(nullable: false),
                        ParentMenuItem_Id = c.Int(),
                    })
                .PrimaryKey(t => t.Id)
                .ForeignKey("dbo.MenuItemInfoModels", t => t.ParentMenuItem_Id)
                .Index(t => t.ParentMenuItem_Id);
            
            CreateTable(
                "dbo.MenuItemInfoModels",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        MenuItemText = c.String(maxLength: 20),
                        ActionName = c.String(maxLength: 50),
                        ControllerName = c.String(maxLength: 50),
                        CreateTime = c.DateTime(nullable: false),
                        UpdateDateTime = c.DateTime(nullable: false),
                        CreateBy = c.String(),
                        UpdateBy = c.String(),
                        IsSoftDelete = c.Boolean(nullable: false),
                        ParentMenuItem_Id = c.Int(),
                    })
                .PrimaryKey(t => t.Id)
                .ForeignKey("dbo.MenuItemInfoModels", t => t.ParentMenuItem_Id)
                .Index(t => t.ParentMenuItem_Id);
            
            CreateTable(
                "dbo.RoleInfoModels",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        RoleName = c.String(maxLength: 50),
                        RoleDescription = c.String(maxLength: 200),
                        CreateTime = c.DateTime(nullable: false),
                        UpdateDateTime = c.DateTime(nullable: false),
                        CreateBy = c.String(),
                        UpdateBy = c.String(),
                        IsSoftDelete = c.Boolean(nullable: false),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.UserInfoModels",
                c => new
                    {
                        Id = c.Int(nullable: false, identity: true),
                        UserName = c.String(maxLength: 30),
                        Password = c.String(),
                        UserState = c.Int(nullable: false),
                        CreateTime = c.DateTime(nullable: false),
                        UpdateDateTime = c.DateTime(nullable: false),
                        CreateBy = c.String(),
                        UpdateBy = c.String(),
                        IsSoftDelete = c.Boolean(nullable: false),
                    })
                .PrimaryKey(t => t.Id);
            
            CreateTable(
                "dbo.RoleInfoModelCommandActionInfoModels",
                c => new
                    {
                        RoleInfoModel_Id = c.Int(nullable: false),
                        CommandActionInfoModel_Id = c.Int(nullable: false),
                    })
                .PrimaryKey(t => new { t.RoleInfoModel_Id, t.CommandActionInfoModel_Id })
                .ForeignKey("dbo.RoleInfoModels", t => t.RoleInfoModel_Id, cascadeDelete: true)
                .ForeignKey("dbo.CommandActionInfoModels", t => t.CommandActionInfoModel_Id, cascadeDelete: true)
                .Index(t => t.RoleInfoModel_Id)
                .Index(t => t.CommandActionInfoModel_Id);
            
            CreateTable(
                "dbo.RoleInfoModelMenuItemInfoModels",
                c => new
                    {
                        RoleInfoModel_Id = c.Int(nullable: false),
                        MenuItemInfoModel_Id = c.Int(nullable: false),
                    })
                .PrimaryKey(t => new { t.RoleInfoModel_Id, t.MenuItemInfoModel_Id })
                .ForeignKey("dbo.RoleInfoModels", t => t.RoleInfoModel_Id, cascadeDelete: true)
                .ForeignKey("dbo.MenuItemInfoModels", t => t.MenuItemInfoModel_Id, cascadeDelete: true)
                .Index(t => t.RoleInfoModel_Id)
                .Index(t => t.MenuItemInfoModel_Id);
            
            CreateTable(
                "dbo.UserInfoModelRoleInfoModels",
                c => new
                    {
                        UserInfoModel_Id = c.Int(nullable: false),
                        RoleInfoModel_Id = c.Int(nullable: false),
                    })
                .PrimaryKey(t => new { t.UserInfoModel_Id, t.RoleInfoModel_Id })
                .ForeignKey("dbo.UserInfoModels", t => t.UserInfoModel_Id, cascadeDelete: true)
                .ForeignKey("dbo.RoleInfoModels", t => t.RoleInfoModel_Id, cascadeDelete: true)
                .Index(t => t.UserInfoModel_Id)
                .Index(t => t.RoleInfoModel_Id);
            
        }
        
        public override void Down()
        {
            DropForeignKey("dbo.UserInfoModelRoleInfoModels", "RoleInfoModel_Id", "dbo.RoleInfoModels");
            DropForeignKey("dbo.UserInfoModelRoleInfoModels", "UserInfoModel_Id", "dbo.UserInfoModels");
            DropForeignKey("dbo.RoleInfoModelMenuItemInfoModels", "MenuItemInfoModel_Id", "dbo.MenuItemInfoModels");
            DropForeignKey("dbo.RoleInfoModelMenuItemInfoModels", "RoleInfoModel_Id", "dbo.RoleInfoModels");
            DropForeignKey("dbo.RoleInfoModelCommandActionInfoModels", "CommandActionInfoModel_Id", "dbo.CommandActionInfoModels");
            DropForeignKey("dbo.RoleInfoModelCommandActionInfoModels", "RoleInfoModel_Id", "dbo.RoleInfoModels");
            DropForeignKey("dbo.CommandActionInfoModels", "ParentMenuItem_Id", "dbo.MenuItemInfoModels");
            DropForeignKey("dbo.MenuItemInfoModels", "ParentMenuItem_Id", "dbo.MenuItemInfoModels");
            DropIndex("dbo.UserInfoModelRoleInfoModels", new[] { "RoleInfoModel_Id" });
            DropIndex("dbo.UserInfoModelRoleInfoModels", new[] { "UserInfoModel_Id" });
            DropIndex("dbo.RoleInfoModelMenuItemInfoModels", new[] { "MenuItemInfoModel_Id" });
            DropIndex("dbo.RoleInfoModelMenuItemInfoModels", new[] { "RoleInfoModel_Id" });
            DropIndex("dbo.RoleInfoModelCommandActionInfoModels", new[] { "CommandActionInfoModel_Id" });
            DropIndex("dbo.RoleInfoModelCommandActionInfoModels", new[] { "RoleInfoModel_Id" });
            DropIndex("dbo.MenuItemInfoModels", new[] { "ParentMenuItem_Id" });
            DropIndex("dbo.CommandActionInfoModels", new[] { "ParentMenuItem_Id" });
            DropTable("dbo.UserInfoModelRoleInfoModels");
            DropTable("dbo.RoleInfoModelMenuItemInfoModels");
            DropTable("dbo.RoleInfoModelCommandActionInfoModels");
            DropTable("dbo.UserInfoModels");
            DropTable("dbo.RoleInfoModels");
            DropTable("dbo.MenuItemInfoModels");
            DropTable("dbo.CommandActionInfoModels");
        }
    }
}