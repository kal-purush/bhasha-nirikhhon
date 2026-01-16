using Com.Danliris.Service.Finance.Accounting.Lib;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.DPPVATBankExpenditureNote;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.DailyBankTransaction;
using Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.Services.JournalTransaction;
using Com.Danliris.Service.Finance.Accounting.Lib.Models.GarmentInvoicePurchasingDisposition;
using Com.Danliris.Service.Finance.Accounting.Lib.Services.HttpClientService;
using Com.Danliris.Service.Finance.Accounting.Lib.Services.IdentityService;
using Com.Danliris.Service.Finance.Accounting.Test.Services.OthersExpenditureProofDocument.Helper;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Com.Danliris.Service.Finance.Accounting.Test.Services
{
    public class DPPVATBankExpenditureNoteServiceTest
    {
        private const string ENTITY = "DPPVATBankExpenditureNoteServiceTest";
        private const int Offset = 7;

        [MethodImpl(MethodImplOptions.NoInlining)]
        protected string GetCurrentMethod()
        {

            var st = new StackTrace();
            var sf = st.GetFrame(1);

            return string.Concat(sf.GetMethod().Name, "_", ENTITY);
        }
        private FinanceDbContext GetDbContext(string testName)
        {
            var serviceProvider = new ServiceCollection()
              .AddEntityFrameworkInMemoryDatabase()
              .BuildServiceProvider();

            var optionsBuilder = new DbContextOptionsBuilder<FinanceDbContext>();
            optionsBuilder
                .UseInMemoryDatabase(testName)
                .ConfigureWarnings(w => w.Ignore(InMemoryEventId.TransactionIgnoredWarning))
                 .UseInternalServiceProvider(serviceProvider);

            var dbContext = new FinanceDbContext(optionsBuilder.Options);

            return dbContext;
        }

        private Mock<IServiceProvider> GetServiceProvider(FinanceDbContext dbContext)
        {
            var serviceProvider = new Mock<IServiceProvider>();

            var documentNoContentResult = new BaseResponse<string>();

            var mockHttpClient = new Mock<IHttpClientService>();
            mockHttpClient
                .Setup(http => http.GetAsync(It.IsAny<string>()))
                .ReturnsAsync(new HttpResponseMessage() { StatusCode = HttpStatusCode.OK, Content = new StringContent(JsonConvert.SerializeObject(documentNoContentResult)) });

            mockHttpClient
                .Setup(http => http.PutAsync(It.IsAny<string>(), It.IsAny<HttpContent>()))
                .ReturnsAsync(new HttpResponseMessage());

            serviceProvider
                .Setup(x => x.GetService(typeof(IHttpClientService)))
                .Returns(mockHttpClient.Object);

            serviceProvider
                .Setup(x => x.GetService(typeof(IIdentityService)))
                .Returns(new IdentityService() { Token = "Token", Username = "Test", TimezoneOffset = Offset });

            serviceProvider
                .Setup(x => x.GetService(typeof(FinanceDbContext)))
                .Returns(dbContext);

            return serviceProvider;
        }

        private FormDto GetForm()
        {
            return new FormDto()
            {
                Amount = 10,
                Bank = new AccountBankDto()
                {
                    AccountNumber = "Number",
                    BankCode = "Code",
                    BankName = "Name",
                    Currency = new CurrencyDto()
                    {
                        Code = "IDR",
                        Id = 1,
                        Rate = 1
                    },
                    Id = 1
                },
                BGCheckNo = "CheckNo",
                Currency = new CurrencyDto()
                {
                    Code = "IDR",
                    Id = 1,
                    Rate = 1
                },
                Date = DateTimeOffset.Now,
                Items = new List<FormItemDto>()
                {
                    new FormItemDto()
                    {
                        InternalNote = new InternalNoteDto()
                        {
                            Currency = new CurrencyDto()
                            {
                                Code = "IDR",
                                Id = 1,
                                Rate = 1
                            },
                            Date = DateTimeOffset.Now,
                            DocumentNo = "DocumentNo",
                            DPP = 10,
                            DueDate = DateTimeOffset.Now,
                            Id = 1,
                            IncomeTaxAmount = 1,
                            Items = new List<InternalNoteInvoiceDto>()
                            {
                                new InternalNoteInvoiceDto()
                                {
                                    Id = 1,
                                    Invoice = new InvoiceDto()
                                    {
                                        Amount = 1,
                                        BillsNo = "BillsNo",
                                        Category = new CategoryDto(1, "Name"),
                                        Date = DateTimeOffset.Now,
                                        DeliveryOrdersNo = "DONo",
                                        DetailDO = new List<DeliveryOrderDto>()
                                        {
                                            new DeliveryOrderDto("doNo", 1, "paymentBill", "billNo", 1, 1)
                                        },
                                        DocumentNo = "InvoiceNo",
                                        Id = 1,
                                        PaymentBills = "Bills",
                                        PaymentMethod = "Credit",
                                        ProductNames = "ProductName"
                                    },
                                    SelectInvoice = true
                                }
                            },
                            Supplier = new SupplierDto()
                            {
                                Code = "Code",
                                Id = 1,
                                IsImport = false,
                                Name = "Name"
                            },
                            TotalAmount = 1,
                            VATAmount = 1
                        },
                        OutstandingAmount = 1,
                        Select = true
                    }
                },
                Supplier = new SupplierDto()
                {
                    Code = "Code",
                    Id = 1,
                    IsImport = false,
                    Name = "Name"
                }
            };
        }

        [Fact]
        public async Task Should_Success_Create()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            Assert.NotEqual(0, id);
        }

        [Fact]
        public async Task Should_Success_Delete_Created()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());

            var result = await service.Delete(id);
            Assert.NotEqual(0, result);
        }

        [Fact]
        public async Task Should_Success_Read_Created_By_Id()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());

            var result = service.Read(id);
            Assert.NotNull(result);
        }

        [Fact]
        public async Task Should_Success_Read_Created()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            var created = service.Read(id);

            var result = service.Read(created.DocumentNo);
            Assert.NotEmpty(result.Data);
        }

        [Fact]
        public async Task Should_Success_Updates_Created()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());

            id = await service.Update(id, GetForm());
            Assert.NotEqual(0, id);
        }

        [Fact]
        public async Task Should_Success_Get_Expenditure_Report()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            var created = service.Read(id);

            var internalNoteId = created.Items.Select(item => item.InternalNote.Id).FirstOrDefault();
            var invoiceId = created.Items.SelectMany(item => item.InternalNote.Items).Select(item => item.Invoice.Id).FirstOrDefault();
            var supplierId = created.Supplier.Id;

            var startDate = DateTimeOffset.MinValue;
            var endDate = DateTimeOffset.MaxValue;

            var result = service.ExpenditureReport(created.Id, internalNoteId, invoiceId, supplierId, startDate, endDate);
            Assert.NotEmpty(result);
        }

        [Fact]
        public async Task Should_Success_Get_Expenditure_Report_Detail()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            var created = service.Read(id);

            var internalNoteId = created.Items.Select(item => item.InternalNote.Id).FirstOrDefault();
            var invoiceId = created.Items.SelectMany(item => item.InternalNote.Items).Select(item => item.Invoice.Id).FirstOrDefault();
            var supplierId = created.Supplier.Id;

            var startDate = DateTimeOffset.MinValue;
            var endDate = DateTimeOffset.MaxValue.AddHours(Offset * -1);

            var result = service.ExpenditureReportDetailDO(created.Id, internalNoteId, invoiceId, supplierId, startDate, endDate);
            Assert.NotEmpty(result);
        }

        [Fact]
        public async Task Should_Success_Get_Expenditure_Report_Detail_Pagination()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            var created = service.Read(id);

            var internalNoteId = created.Items.Select(item => item.InternalNote.Id).FirstOrDefault();
            var invoiceId = created.Items.SelectMany(item => item.InternalNote.Items).Select(item => item.Invoice.Id).FirstOrDefault();
            var supplierId = created.Supplier.Id;

            var startDate = DateTimeOffset.MinValue;
            var endDate = DateTimeOffset.MaxValue.AddHours(Offset * -1);

            var result = service.ExpenditureReportDetailDOPagination(1, 10, created.Id, internalNoteId, invoiceId, supplierId, startDate, endDate);
            Assert.NotEmpty(result);
        }

        [Fact]
        public async Task Should_Success_Posting()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());

            var result = await service.Posting(new List<int>() { id });
            Assert.NotEqual(0, result);
        }

        [Fact]
        public async Task Should_Success_Get_Expenditure_Report_From_Invoice()
        {
            var dbContext = GetDbContext(GetCurrentMethod());

            var serviceProviderMock = GetServiceProvider(dbContext);

            var service = new DPPVATBankExpenditureNoteService(serviceProviderMock.Object);

            var id = await service.Create(GetForm());
            var created = service.Read(id);

            var internalNoteId = created.Items.Select(item => item.InternalNote.Id).FirstOrDefault();
            var invoiceId = created.Items.SelectMany(item => item.InternalNote.Items).Select(item => item.Invoice.Id).FirstOrDefault();
            
            var result = service.ExpenditureFromInvoice(invoiceId);
            Assert.NotNull(result);
        }
    }
}