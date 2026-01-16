using AutoMapper;
using EST.MIT.InvoiceImporter.Function.Models;
using EST.MIT.InvoiceImporter.Function.TableEntities;
using EST.MIT.InvoiceImporter.Function.AutoMapperProfiles;

namespace EST.MIT.InvoiceImporter.Function.Test.AutoMapperProfiles;

public class ImportRequestMapperTests
{
    private IMapper _mapper;

    public ImportRequestMapperTests()
    {
        var config = new MapperConfiguration(cfg => cfg.AddProfile<ImportRequestMapper>());
        _mapper = config.CreateMapper();
    }

    [Fact]
    public void Test_ImportRequestEntity_To_ImportRequest_Mapping()
    {
        var entity = new ImportRequestEntity
        {
            PartitionKey = "f3939c6a-3527-4c0a-a649-f662f116d296",
            FileName = "test.xlsx",
            FileSize = 1024,
            FileType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Timestamp = DateTimeOffset.Now,
            PaymentType = "AR",
            Organisation = "RDT",
            SchemeType = "CP",
            AccountType = "First Payment",
            CreatedBy = "test@example.com",
            Status = UploadStatus.Uploaded
        };

        var model = _mapper.Map<ImportRequest>(entity);

        Assert.Equal(entity.FileName, model.FileName);
        Assert.Equal(Guid.Parse(entity.PartitionKey), model.ImportRequestId);
        Assert.Equal(entity.FileSize, model.FileSize);
        Assert.Equal(entity.FileType, model.FileType);
        Assert.Equal(entity.Timestamp, model.Timestamp);
        Assert.Equal(entity.PaymentType, model.PaymentType);
        Assert.Equal(entity.Organisation, model.Organisation);
        Assert.Equal(entity.SchemeType, model.SchemeType);
        Assert.Equal(entity.AccountType, model.AccountType);
        Assert.Equal(entity.CreatedBy, model.CreatedBy);
        Assert.Equal(entity.Status, model.Status);
    }

    [Fact]
    public void Test_ImportRequest_To_ImportRequestEntity_Mapping()
    {
        var model = new ImportRequest
        {
            ImportRequestId = Guid.Parse("f3939c6a-3527-4c0a-a649-f662f116d296"),
            FileName = "test.xlsx",
            FileSize = 1024,
            FileType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            Timestamp = DateTimeOffset.Now,
            PaymentType = "AR",
            Organisation = "RDT",
            SchemeType = "CP",
            AccountType = "First Payment",
            CreatedBy = "test@example.com",
            Status = UploadStatus.Uploaded,
            BlobPath = "https://defrastorageaccount.blob.core.windows.net/invoices/import/test.xlsx"
        };

        var entity = _mapper.Map<ImportRequestEntity>(model);

        Assert.Equal(model.FileName, entity.FileName);
        Assert.Equal(model.ImportRequestId.ToString(), entity.PartitionKey);
        Assert.Equal($"{model.ImportRequestId}_{model.Timestamp:O}", entity.RowKey);
        Assert.Equal(model.FileSize, entity.FileSize);
        Assert.Equal(model.FileType, entity.FileType);
        Assert.Equal(model.Timestamp, entity.Timestamp);
        Assert.Equal(model.PaymentType, entity.PaymentType);
        Assert.Equal(model.Organisation, entity.Organisation);
        Assert.Equal(model.SchemeType, entity.SchemeType);
        Assert.Equal(model.AccountType, entity.AccountType);
        Assert.Equal(model.CreatedBy, entity.CreatedBy);
        Assert.Equal(model.Status, entity.Status);
    }
}