using System;
using System.Linq;
using System.Threading.Tasks;
using FurnitureShop.Core.Contracts;
using FurnitureShop.Core.Contracts.Mobile.Complaints;
using FurnitureShop.Core.Domain;
using FurnitureShop.Core.Services.CQRS;
using FurnitureShop.Core.Services.CQRS.Mobile.Complaints;
using FurnitureShop.Core.Services.DataAccess;
using LeanCode.CQRS;
using LeanCode.DomainModels.Model;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace FurnitureShop.Core.Services.Tests.CQRS.Mobile
{
    public class ComplaintTests : IDisposable
    {
        private readonly Order TestOrder = new Order("test_street", "test_city",
            "test_state", "test_postal_code", "test_country")
        {
            UserId = Id<User>.From(TestUserId),
        };
        private readonly Complaint TestComplaint = new Complaint("test_Complaint")
        {
            Resolved = true,
            Response = "test response",
            CreatedDate = DateTime.Parse("09-09-1999")
        };
        private static Guid TestUserId = Guid.Parse("5d60120d-8a32-47f1-8b81-4018eb230b19");

        private string NewComplaintText = "new Complaint";
        private string NewComplaintResponse = "new response";
        private bool NewComplaintResolved = false;
        private DateTime NewComplaintCreatedDate = DateTime.Parse("09-04-2011");
        private readonly string TestUserRole = Auth.Roles.User;
        private readonly string TestAdminRole = Auth.Roles.Admin;
        private DbContextOptions<CoreDbContext> ContextOptions { get; }
        private void Seed()
        {
            using var context = new CoreDbContext(ContextOptions);
            context.Orders.Add(TestOrder);
            context.SaveChanges();

            TestComplaint.OrderId = context.Orders.Where(c => c.UserId == TestUserId).FirstOrDefault().Id;
            context.Complaints.Add(TestComplaint);
            context.SaveChanges();
        }
        public void Dispose()
        {
            using var context = new CoreDbContext(ContextOptions);
            context.Database.EnsureDeleted();
        }

        public ComplaintTests()
        {
            ContextOptions = new DbContextOptionsBuilder<CoreDbContext>()
                    .UseInMemoryDatabase(Guid.NewGuid().ToString())
                    .Options;
            Seed();
        }

        [Fact]
        public void ComplaintByIdQHTest()
        {
            var coreContext = CoreContext.ForTests(TestUserId, TestUserRole);
            using var dbContext = new CoreDbContext(ContextOptions);
            var handler = new ComplaintByIdQH(dbContext);
            var command = new ComplaintById { Id = TestComplaint.Id };
            
            var result = handler.ExecuteAsync(coreContext, command);
            
            Assert.True(result.IsCompletedSuccessfully);
            var Complaint = result.Result;
            Assert.NotNull(Complaint);
            Assert.Equal(TestComplaint.Text, Complaint.ComplaintInfo.Text);
            Assert.Equal(TestComplaint.Response, Complaint.ComplaintInfo.Response);
            Assert.Equal(TestComplaint.Resolved, Complaint.ComplaintInfo.Resolved);
            Assert.Equal(TestComplaint.OrderId, Complaint.ComplaintInfo.OrderId);
            Assert.Equal(TestComplaint.UserId, Complaint.ComplaintInfo.UserId);
            Assert.Equal(TestComplaint.Id, Complaint.Id);
        }
        [Fact]
        public void CreateComplaintTest()
        {
            var coreContext = CoreContext.ForTests(TestUserId, TestAdminRole);
            using var dbContext = new CoreDbContext(ContextOptions);
            var handler = new CreateComplaintCH(dbContext);
            var command = new CreateComplaint
            {
                ComplaintInfo = new ComplaintInfoDTO
                {
                    Text = NewComplaintText,
                    Resolved = NewComplaintResolved,
                    UserId = TestUserId,
                    OrderId = TestOrder.Id
                }
            };
            
            var result = handler.ExecuteAsync(coreContext, command);
            
            Assert.True(result.IsCompletedSuccessfully);
            var Complaint = dbContext.Complaints.Where(c => c.Text == NewComplaintText).FirstOrDefault();
            Assert.NotNull(Complaint);
            Assert.Equal(NewComplaintText, Complaint.Text);
            Assert.Equal(NewComplaintResolved, Complaint.Resolved);
            Assert.Equal(TestOrder.Id, Complaint.OrderId);
        }
        [Fact]
        public void DeleteComplaintTest()
        {
            var coreContext = CoreContext.ForTests(TestUserId, TestAdminRole);
            using var dbContext = new CoreDbContext(ContextOptions);
            var handler = new DeleteComplaintCH(dbContext);
            var command = new DeleteComplaint { Id = TestComplaint.Id };
            
            var result = handler.ExecuteAsync(coreContext, command);
            
            Assert.True(result.IsCompletedSuccessfully);
            var Complaint = dbContext.Complaints.Find(TestComplaint.Id);
            Assert.Null(Complaint);
        }
        [Fact]
        public void UpdateComplaintTest()
        {
            var coreContext = CoreContext.ForTests(TestUserId, TestAdminRole);
            using var dbContext = new CoreDbContext(ContextOptions);
            var handler = new UpdateComplaintCH(dbContext);
            var command = new UpdateComplaint
            {
                Id = TestComplaint.Id,
                ComplaintInfo = new ComplaintInfoDTO
                {
                    Text = NewComplaintText,
                    Response = NewComplaintResponse,
                    Resolved = NewComplaintResolved,
                    UserId = TestUserId,
                    OrderId = TestOrder.Id
                }
            };
            
            var result = handler.ExecuteAsync(coreContext, command);
            
            Assert.True(result.IsCompletedSuccessfully);
            var Complaint = dbContext.Complaints.Where(c => c.Text == NewComplaintText).FirstOrDefault();
            Assert.NotNull(Complaint);
            Assert.Equal(NewComplaintText, Complaint.Text);
            Assert.Equal(NewComplaintResponse, Complaint.Response);
            Assert.Equal(NewComplaintResolved, Complaint.Resolved);
            Assert.Equal(TestOrder.Id, Complaint.OrderId);
        }
    }
}