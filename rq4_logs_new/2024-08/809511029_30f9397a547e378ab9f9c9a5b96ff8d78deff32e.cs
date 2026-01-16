using Application.Exceptions;
using Application.Repositories;
using Domain.Entities;
using Domain.Enums;
using Domain.Exceptions;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using static Application.Commands.CreateGroupInvoice;

namespace UniteTest.CreateHandlerTests
{
    public class CreateGroupInvoiceTest
    {
        private readonly Mock<IMemberRepository> _memberRepositoryMock;
        private readonly Mock<IChandaTypeRepository> _chandaTypeRepositoryMock;
        private readonly Mock<IInvoiceRepository> _invoiceRepositoryMock;
        private readonly Mock<ICurrentUser> _currentUserMock;
        private readonly Mock<IUnitOfWork> _unitOfWorkMock;
        private readonly Handler _handler;

        public CreateGroupInvoiceTest()
        {
            _memberRepositoryMock = new Mock<IMemberRepository>();
            _chandaTypeRepositoryMock = new Mock<IChandaTypeRepository>();
            _invoiceRepositoryMock = new Mock<IInvoiceRepository>();
            _currentUserMock = new Mock<ICurrentUser>();
            _unitOfWorkMock = new Mock<IUnitOfWork>();

            _handler = new Handler(
                _unitOfWorkMock.Object,
                _currentUserMock.Object,
                _memberRepositoryMock.Object,
                _invoiceRepositoryMock.Object,                
                _chandaTypeRepositoryMock.Object
            );
        }

        [Fact]
        public async Task Handle_NoValidChandaTypeSelected_ShouldThrowAnException()
        {
            // Arrange
            var circuit = new Circuit("Abeokuta", "ABK", "0001");
            var jamaat = new Jamaat("Lafiaji", "ABK-L", circuit.Id, "0001");
            var memberLedger = new MemberLedger(Guid.NewGuid(), "0001");
            var member = new Member("0001", "Usman Tijani", "johndoe@mail.com", "08011111111", jamaat.Id, memberLedger.Id, "0001");
            var chandaType = new ChandaType("Chanda Wasiyyat", "CHA-WAS", "Chanda Wasiyyat", new Guid("e041f7a3-7b3e-411c-a679-428ba1b1a884"), "0001");


            var command = new Command
            {
                InvoiceItems = new List<InvoiceItemCommand>
                {
                    new InvoiceItemCommand
                    {
                        PayerNo = "0001",
                        ReceiptNo = "REC-123",
                        MonthPaidFor = MonthOfTheYear.January,
                        Year = 2024,
                        ChandaItems = new List<ChandaItemCommand>
                        {
                            new ChandaItemCommand( chandaType.Code, 100m )
                        }
                    }
                }
            };

            _chandaTypeRepositoryMock.Setup(ct => ct.GetChandaTypes(It.IsAny<List<string>>()))
                .Returns(new List<ChandaType>());

            // Act & Assert
            var exception = await Assert.ThrowsAsync<NotFoundException>(() => _handler.Handle(command, CancellationToken.None));
            Assert.Equal(ExceptionCodes.NoValidChandaTypeSelected.ToString(), exception.ErrorCode.ToString());
        }

        [Fact]
        public async Task Handle_ValidCommand_ShouldReturnInvoiceResponse()
        {
            // Arrange
            var circuit = new Circuit("Abeokuta", "ABK", "0001");
            var jamaat = new Jamaat("Lafiaji", "ABK-L", circuit.Id, "0001");
            var memberLedger = new MemberLedger(Guid.NewGuid(), "0001");
            var member = new Member("0001", "Usman Tijani", "johndoe@mail.com", "08011111111", jamaat.Id, memberLedger.Id, "0001");
            var chandaType = new ChandaType("Chanda Wasiyyat", "CHA-WAS", "Chanda Wasiyyat", new Guid("e041f7a3-7b3e-411c-a679-428ba1b1a884"), "0001");


            var command = new Command
            {
                InvoiceItems = new List<InvoiceItemCommand>
                {
                    new InvoiceItemCommand
                    {
                        PayerNo = "0001",
                        ReceiptNo = "REC-123",
                        MonthPaidFor = MonthOfTheYear.January,
                        Year = 2024,
                        ChandaItems = new List<ChandaItemCommand>
                        {
                            new ChandaItemCommand( chandaType.Code, 100m )
                        }
                    }
                }
            };

            _chandaTypeRepositoryMock.Setup(c => c.GetChandaTypes(It.IsAny<List<string>>()))
                .Returns(new List<ChandaType> { chandaType });

            _memberRepositoryMock.Setup(m => m.GetMembers(It.IsAny<Expression<Func<Member, bool>>>()))
                .Returns(new List<Member> { member });

            _invoiceRepositoryMock.Setup(i => i.AddAsync(It.IsAny<Invoice>()))
                .ReturnsAsync(new Invoice(new Guid("e041f7a3-7b3e-411c-a679-428ba1b1a884"), jamaat.Id, "INV-AE23GHS", 100m, InvoiceStatus.Pending, member.ChandaNo));

            // Act
            var result = await _handler.Handle(command, CancellationToken.None);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(result.Amount, command.InvoiceItems
                .SelectMany(item => item.ChandaItems).Sum(chanda => chanda.Amount));
            Assert.Equal(result.Status.ToString(), InvoiceStatus.Pending.ToString());            
        }
    }
}