using cursuri_studenti.admin.model;
using cursuri_studenti.admin.service;
using cursuri_studenti.enrolment.model;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace tests_cursuri_studenti.Tests
{
    public class TestAdmin
    {
        // Testing Admin Service

        [Fact]
        public void FindById_ValidMatch_ReturnsAdmin()
        {
            // Arrange
            int Id = 123;
            Admin expectedAdmin = new Admin(Id, "test name", "test@test.com", "test123");
            List<Admin> list = new List<Admin> { expectedAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindById(Id);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void FindById_NoMatch_ReturnsNull()
        {
            // Arrange
            int id = 123;
            List<Admin> list = new List<Admin>();
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindById(id);

            // Assert
            Assert.Null(actualAdmin);
        }

        [Fact]
        public void FindById_MultipleAdmins_ReturnsCorrectAdmin()
        {
            // Arrange
            int Id = 123;
            Admin expectedAdmin = new Admin(Id, "test name", "test@test.com", "test123");
            Admin anotherAdmin = new Admin(101, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { expectedAdmin, anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindById(Id);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void FindByEmail_ValidMatch_ReturnsAdmin()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            Admin expectedAdmin = new Admin(12, "Admin Name", Email, "justAPassword");
            List<Admin> list = new List<Admin> { expectedAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmail(Email);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void FindByEmail_NoMatch_ReturnNull()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            List<Admin> list = new List<Admin>();
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmail(Email);

            // Assert
            Assert.Null(actualAdmin);
        }

        [Fact]
        public void FindByEmail_MultipleAdmins_ReturnsCorrectAdmin()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            Admin expectedAdmin = new Admin(12, "Admin Name", Email, "justAPassword");
            Admin anotherAdmin = new Admin(101, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { expectedAdmin, anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmail(Email);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void FindByEmailAndPassword_ValidMatch_ReturnsAdmin()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            string Password = "justAPassword";
            Admin expectedAdmin = new Admin(12, "Admin Name", Email, Password);
            List<Admin> list = new List<Admin> { expectedAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmailAndPassword(Email, Password);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void FindByEmailAndPassword_NoMatch_ReturnsNull()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            string Password = "justAPassword";
            List<Admin> list = new List<Admin>();
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmailAndPassword(Email, Password);

            // Assert
            Assert.Null(actualAdmin);
        }

        [Fact]
        public void FindByEmailAndPassword_MultipleAdmins_ReturnsCorrectAdmin()
        {
            // Arrange
            string Email = "emailforTest@test.com";
            string Password = "justAPassword";
            Admin expectedAdmin = new Admin(12, "Admin Name", Email, Password);
            Admin anotherAdmin = new Admin(101, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { expectedAdmin, anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin actualAdmin = adminService.FindByEmailAndPassword(Email, Password);

            // Assert
            Assert.NotNull(actualAdmin);
            Assert.Equal(expectedAdmin.Id, actualAdmin.Id);
            Assert.Equal(expectedAdmin.Name, actualAdmin.Name);
            Assert.Equal(expectedAdmin.Email, actualAdmin.Email);
            Assert.Equal(expectedAdmin.Password, actualAdmin.Password);
        }

        [Fact]
        public void NewId_ReturnsUnusedId()
        {
            // Arrange
            int Id1 = 12, Id2 = 101;
            Admin anAdmin = new Admin(Id1, "Admin Name", "email@mail.com", "pass");
            Admin anotherAdmin = new Admin(Id2, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { anAdmin, anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            int newId = adminService.NewId();

            // Assert
            Assert.Null(adminService.FindById(newId));
            Assert.NotEqual(Id1, newId);
            Assert.NotEqual(Id2, newId);
            Assert.InRange(newId, 1, 9999);
        }

        [Fact]
        public void RemoveById_ValidMatch_RemovesAdmin_ReturnsTrue()
        {
            // Arrange
            int Id = 12;
            Admin anAdmin = new Admin(Id, "Admin Name", "email@mail.com", "pass");
            List<Admin> list = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            bool removed = adminService.RemoveById(Id);
            list.Remove(anAdmin);

            // Assert
            Assert.True(removed);
            Assert.Null(adminService.FindById(Id));
        }

        [Fact]
        public void RemoveById_NoMatch_DoesntRemoveAdmin_ReturnsFalse()
        {
            // Arrange
            int Id = 12;
            Admin anotherAdmin = new Admin(101, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            bool removed = adminService.RemoveById(Id);

            // Assert
            Assert.False(removed);
            Assert.Equal(list, adminService.GetList());
        }

        [Fact]
        public void RemoveById_MultipleAdmins_RemovesCorrectAdmin_ReturnsTrue()
        {
            // Arrange
            int Id = 12;
            Admin anAdmin = new Admin(Id, "Admin Name", "email@mail.com", "pass");
            Admin anotherAdmin = new Admin(101, "just another admin", "testAdmin@email.xyz", "password");
            List<Admin> list = new List<Admin> { anAdmin, anotherAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            bool removed = adminService.RemoveById(Id);
            list.Remove(anAdmin);

            // Assert
            Assert.True(removed);
            Assert.Null(adminService.FindById(Id));
        }

        [Fact]
        public void AddAdmin_NoMatchingEmail_AddsAdmin_ReturnsTrue()
        {
            // Arrange
            int Id = 281;
            Admin anAdmin = new Admin(Id, "admin", "email@email.to", "passw");
            List<Admin> list = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            string Email = "added@email.com";
            Admin addedAdmin = new Admin(0, "added admin", Email, "passwordAdded");
            bool added = adminService.AddAdmin(addedAdmin);

            // Assert
            Assert.True(added);
            Assert.NotNull(adminService.FindByEmail(Email));
        }

        [Fact]
        public void AddAdmin_MatchingEmail_DoesntAddAdmin_ReturnsFalse()
        {
            // Arrange
            int Id = 281;
            string Email = "email@email.to";
            Admin anAdmin = new Admin(Id, "admin", Email, "passw");
            List<Admin> list = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            Admin addedAdmin = new Admin(0, "added admin", Email, "password2");
            bool added = adminService.AddAdmin(addedAdmin);

            // Assert
            Assert.False(added);
            Assert.Equal(anAdmin, adminService.FindByEmail(Email));
            Assert.Equal(list, adminService.GetList());
            Assert.Equal(list.Count(), adminService.GetCount());
        }

        [Fact]
        public void GetCount_ReturnsActualCount()
        {
            // Arrange
            Admin anAdmin = new Admin(123, "admin", "email@mail.com", "passw");
            List<Admin> list = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            int count = adminService.GetCount();

            // Assert
            Assert.Equal(list.Count(), count);
        }

        [Fact]
        public void ClearList_ListRemainsEmpty()
        {
            // Arrange
            Admin anAdmin = new Admin(123, "admin", "email@mail.com", "passw");
            List<Admin> list = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            adminService.ClearList();

            // Assert
            Assert.Equal(0, adminService.GetCount());
            Assert.Empty(adminService.GetList());
        }

        [Fact]
        public void GetList_ReturnsActualList()
        {
            // Arrange
            Admin anAdmin = new Admin(123, "admin", "email@mail.com", "passw");
            List<Admin> expectedList = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(expectedList);

            // Act
            List<Admin> actualList = adminService.GetList();

            // Assert
            Assert.Equal(expectedList, actualList);
            Assert.Equal(expectedList.Count(), actualList.Count());
        }

        [Fact]
        public void SetList_EditsList()
        {
            // Arrange
            Admin anAdmin = new Admin(123, "admin", "email@mail.com", "passw");
            List<Admin> list = new List<Admin>();
            List<Admin> setList = new List<Admin> { anAdmin };
            AdminService adminService = new AdminService(list);

            // Act
            adminService.SetList(setList);

            // Assert
            Assert.Equal(setList, adminService.GetList());
            Assert.Equal(setList.Count(), adminService.GetCount());
            Assert.NotEqual(list, adminService.GetList());
        }
    }
}