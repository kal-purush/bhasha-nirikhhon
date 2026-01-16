using DeliveryApp.Core.Domain.CourierAggregate;
using DeliveryApp.Infrastructure;
using DeliveryApp.Infrastructure.Adapters.Postgres.Couriers;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using Xunit;

namespace DeliveryApp.IntegrationTests.Repositories;

public class CourierRepositoryShould : IAsyncLifetime
{
    private ApplicationDbContext _context;

    /// <summary>
    /// Настройка Postgres из библиотеки TestContainers
    /// </summary>
    /// <remarks>По сути это Docker контейнер с Postgres</remarks>
    private readonly PostgreSqlContainer _postgreSqlContainer =
        new PostgreSqlBuilder()
            .WithImage("postgres:14.7")
            .WithDatabase("order")
            .WithUsername("username")
            .WithPassword("secret")
            .WithCleanUp(true)
            .Build();

    /// <summary>
    /// Ctr
    /// </summary>
    /// <remarks>Вызывается один раз перед всеми тестами в рамках этого класса</remarks>
    public CourierRepositoryShould()
    {
    }

    /// <summary>
    /// Инициализируем окружение
    /// </summary>
    /// <remarks>Вызывается перед каждым тестом</remarks>
    public async Task InitializeAsync()
    {
        //Стартуем БД (библиотека TestContainers запускает Docker контейнер с Postgres)
        await _postgreSqlContainer.StartAsync();

        //Накатываем миграции и справочники
        var contextOptions = new DbContextOptionsBuilder<ApplicationDbContext>().UseNpgsql(
            _postgreSqlContainer.GetConnectionString(),
            npgsqlOptionsAction: sqlOptions =>
            {
                sqlOptions.MigrationsAssembly("DeliveryApp.Infrastructure");
            }).Options;
        _context = new ApplicationDbContext(contextOptions);
        await _context.Database.MigrateAsync();
    }

    /// <summary>
    /// Уничтожаем окружение
    /// </summary>
    /// <remarks>Вызывается после каждого теста</remarks>
    public async Task DisposeAsync()
    {
        await _postgreSqlContainer.DisposeAsync().AsTask();
    }

    [Fact]
    public async void CanAddCourier()
    {
        //Arrange
        var courier = Courier.Create(Guid.NewGuid(), "Иван", Transport.Pedestrian);

        //Act
        var courierRepository = new CourierRepository(_context);
        var unitOfWork = new UnitOfWork(_context);
        courierRepository.Add(courier);
        await unitOfWork.SaveEntitiesAsync();

        //Assert
        var courierFromDb = await courierRepository.Get(courier.Id);
        courier.Should().BeEquivalentTo(courierFromDb);
    }

    [Fact]
    public async void CanUpdateCourier()
    {
        //Arrange
        var courier = Courier.Create(Guid.NewGuid(), "Иван", Transport.Pedestrian);

        var courierRepository = new CourierRepository(_context);
        var unitOfWork = new UnitOfWork(_context);
        courierRepository.Add(courier);
        await unitOfWork.SaveEntitiesAsync();

        //Act
        courier.GetOnTheLine();
        courierRepository.Update(courier);
        await unitOfWork.SaveEntitiesAsync();

        //Assert
        var courierFromDb = await courierRepository.Get(courier.Id);
        courier.Should().NotBeNull();
        courier.Should().BeEquivalentTo(courierFromDb);
        courierFromDb!.Status.Should().Be(CourierStatus.Ready);
    }

    [Fact]
    public async void CanGetById()
    {
        //Arrange
        var courier = Courier.Create(Guid.NewGuid(), "Иван", Transport.Pedestrian);

        //Act
        var courierRepository = new CourierRepository(_context);
        var unitOfWork = new UnitOfWork(_context);
        courierRepository.Add(courier);
        await unitOfWork.SaveEntitiesAsync();

        //Assert
        var courierFromDb = await courierRepository.Get(courier.Id);
        courier.Should().BeEquivalentTo(courierFromDb);
    }

    // [Fact]
    // public async void CanGetAllActive()
    // {
    //     //Arrange
    //     var courier1 = Courier.Create(Guid.NewGuid(), "Иван", Transport.Pedestrian);
    //     courier1.GetOffTheLine();
    //
    //     var courier2 = Courier.Create(Guid.NewGuid(), "Борис", Transport.Pedestrian);
    //     courier2.GetOnTheLine();
    //
    //     var courierRepository = new CourierRepository(_context);
    //     var unitOfWork = new UnitOfWork(_context);
    //     courierRepository.Add(courier1);
    //     courierRepository.Add(courier2);
    //     await unitOfWork.SaveEntitiesAsync();
    //
    //     //Act
    //     var activeCouriersFromDb = courierRepository.GetAllReady();
    //
    //     //Assert
    //     var couriersFromDb = activeCouriersFromDb.ToList();
    //     couriersFromDb.Should().NotBeEmpty();
    //     couriersFromDb.Count().Should().Be(1);
    //     couriersFromDb.First().Should().BeEquivalentTo(courier2);
    // }
}