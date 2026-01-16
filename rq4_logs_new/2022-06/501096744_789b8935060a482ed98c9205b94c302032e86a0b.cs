using FoodZone.Models.Common;
using Microsoft.AspNet.Identity.EntityFramework;
using Microsoft.AspNet.Identity;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using FoodZone.Models.Security;

namespace FoodZone.Data
{
    internal class DbInitializer : DropCreateDatabaseIfModelChanges<FoodZoneContext>
    {
        protected override void Seed(FoodZoneContext context)
        {
            InitializeIdentity(context);

            var menu = new List<Menu>
            {
                new Menu
                {
                    Id = 1,
                    Name = "Đồ Uống",
                    Description = "Mang lại một vị tươi mới",
                    InsertedAt = DateTime.Now
                },
                new Menu
                {
                    Id = 2,
                    Name = "Thực Đơn Theo Mùa",
                    Description = "Mỗi mùa mang đến một vị khác nhau",
                    InsertedAt = DateTime.Now
                },
                new Menu
                {
                    Id = 3,
                    Name = "Thực Đơn Tối",
                    Description = "Mùa nào cũng có",
                    InsertedAt = DateTime.Now
                },
                new Menu
                {
                    Id = 4,
                    Name = "Thực Đơn Sáng",
                    Description = "Mùa nào cũng có",
                    InsertedAt = DateTime.Now
                }
            };

            context.Menus.AddRange(menu);

            var foods = new List<Food>
            {
                new Food
                {
                    Id = 1,
                    Name = "Trà Đào Cam Sả",
                    Description = "Có vị thanh ngọt của đào, vị chua dịu của những tép cam căng mọng, vị chát của trà, cùng hương thơm nồng đặc trưng của sả.",
                    Price = 30000,
                    Image = "tradaocamxa.jpg",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 2,
                    Name = "Sữa Chua Đánh Đá",
                    Description = "Vị chua chua hòa quyện với vị ngọt từ hoa quả cùng cảm giác mát lạnh từ món ăn.",
                    Price = 30000,
                    Image = "suachuadanhda.jpg",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 3,
                    Name = "Thịt Bò Kho Cà Rốt",
                    Description = "Thịt bò được kho vừa mềm mà không bị dai, cà rốt giòn ngọt thấm vị nước sốt cay cay mặn mặn đậm đà, dậy mùi thơm phức từ sả rất hấp dẫn.",
                    Price = 50000,
                    Image = "thitbokhocarot.jpg",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 4,
                    Name = "Mực Viên Tuyết Hoa",
                    Description = "Mùi vị thơm ngon và độ dai giòn của mực hòa với vị ngon của nhân bên trong. ",
                    Price = 215000,
                    Image = "mucvientuyethoa.jpg",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 5,
                    Name = "Cánh Gà Nướng Rau Củ",
                    Description = "Ướp với nước sốt đặc biệt làm món ăn ngon ngọt thơm.",
                    Price = 150000,
                    Image = "canhganuongraucu.png",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 6,
                    Name = "Sườn Heo Om Dứa",
                    Description = "Vị thơm của dứa thêm vào cho sườn heo một vị chua chua.",
                    Price = 170000,
                    Image = "suonheoomdua.png",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 7,
                    Name = "Cà Tím Hấp Thịt Heo",
                    Description = "Vị thơm của cà thêm vào cho thịt heo vị ngon.",
                    Price = 130000,
                    Image = "catimhapthitheo.png",
                    InsertedAt = DateTime.Now
                },
                new Food
                {
                    Id = 8,
                    Name = "Đậu Phụ Om Với Tôm",
                    Description = "Vị thơm của cà thêm vào cho thịt heo vị ngon.",
                    Price = 120000,
                    Image = "dauphuomvoitom.png",
                    InsertedAt = DateTime.Now
                }
            };

            context.Foods.AddRange(foods);

            var menuFood = new List<MenuFood>
            {
                new MenuFood
                {
                    Id = 1,
                    Food = foods.Single(x=>x.Name == "Trà Đào Cam Sả"),
                    Menu = menu.Single(x=>x.Name == "Đồ Uống"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 2,
                    Food = foods.Single(x=>x.Name == "Sữa Chua Đánh Đá"),
                    Menu = menu.Single(x=>x.Name == "Đồ Uống"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 3,
                    Food = foods.Single(x=>x.Name == "Thịt Bò Kho Cà Rốt"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Theo Mùa"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 4,
                    Food = foods.Single(x=>x.Name == "Mực Viên Tuyết Hoa"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Theo Mùa"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 5,
                    Food = foods.Single(x=>x.Name == "Cánh Gà Nướng Rau Củ"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Theo Mùa"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 6,
                    Food = foods.Single(x=>x.Name == "Cánh Gà Nướng Rau Củ"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Tối"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 7,
                    Food = foods.Single(x=>x.Name == "Sườn Heo Om Dứa"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Tối"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 8,
                    Food = foods.Single(x=>x.Name == "Cà Tím Hấp Thịt Heo"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Tối"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 9,
                    Food = foods.Single(x=>x.Name == "Đậu Phụ Om Với Tôm"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Sáng"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 10,
                    Food = foods.Single(x=>x.Name == "Cà Tím Hấp Thịt Heo"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Sáng"),
                    InsertedAt = DateTime.Now
                },
                new MenuFood
                {
                    Id = 11,
                    Food = foods.Single(x=>x.Name == "Sườn Heo Om Dứa"),
                    Menu = menu.Single(x=>x.Name == "Thực Đơn Sáng"),
                    InsertedAt = DateTime.Now
                }
            };

            context.MenuFoods.AddRange(menuFood);

            context.SaveChanges();
        }

        public static void InitializeIdentity(FoodZoneContext db)
        {
            var userManager = new UserManager<User>(new UserStore<User>(db));
            var roleManager = new RoleManager<IdentityRole>(new RoleStore<IdentityRole>(db));
            const string email = "admin@example.com";
            const string password = "admin123";
            const string roleName = "Manager";

            //Create Role Manager if it does not exist
            var role = roleManager.FindByName(roleName);
            if (role == null)
            {
                role = new IdentityRole(roleName);
                var roleResult = roleManager.Create(role);
            }

            //Create Role User if it does not exist
            var roleUser = roleManager.FindByName("User");
            if (roleUser == null)
            {
                roleUser = new IdentityRole("User");
                var roleResult = roleManager.Create(roleUser);
            }

            //Create Role Staff if it does not exist
            var roleStaff = roleManager.FindByName("Staff");
            if (roleStaff == null)
            {
                roleStaff = new IdentityRole("Staff");
                var roleResult = roleManager.Create(roleStaff);
            }

            var user = userManager.FindByName(email);
            if (user == null)
            {
                user = new User { UserName = "Manager", Email = email, Level = 0, PhoneNumber="0985786750"};
                var result = userManager.Create(user, password);
                result = userManager.SetLockoutEnabled(user.Id, false);
            }

            // Add user admin to Role Manager if not already added
            var rolesForUser = userManager.GetRoles(user.Id);
            if (!rolesForUser.Contains(role.Name))
            {
                var result = userManager.AddToRole(user.Id, role.Name);
            }
        }
    }
}