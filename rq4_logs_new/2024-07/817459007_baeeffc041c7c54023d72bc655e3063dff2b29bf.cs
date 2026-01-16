using BlazorEcommerce.Server;

namespace WebServerSeeding.Data
{
    public class AppDbInitializer
    {
        // Initialize Seed method with IApplicationBuilder as argument to create a Scope application service
        public static void Seed(IApplicationBuilder applicationBuilder)
        {
            using (var serviceScope = applicationBuilder.ApplicationServices.CreateScope())
            {
                // Create context and get the AppDbContext Service
                var context = serviceScope.ServiceProvider.GetService<AppDbContext>();
                // Verify that db is created, if not create automatically using ensure created
                context.Database.EnsureCreated();

                // add Locations if there are nothing in Database
                if (!context.Locations.Any())
                {
                    var locations = new List<Location>
                    {
                        new Location
                        {
                            Id = Guid.NewGuid(),
                            Name = "Warehouse A",
                            Address = "123 Main St, City A"
                        },
                        new Location
                        {
                            Id = Guid.NewGuid(),
                            Name = "Warehouse B",
                            Address = "456 Elm St, City B"
                        },
                        new Location
                        {
                            Id = Guid.NewGuid(),
                            Name = "Warehouse C",
                            Address = "789 Oak St, City C"
                        }
                    };
                    context.Locations.AddRange(locations);
                    context.SaveChanges();

                    // add Products if there are nothing in Database
                    if (!context.Products.Any())
                    {
                        var products = new List<Product>
                        {
                            new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Laptop",
                            ShortDescription = "High-performance laptop",
                            Description = "A high-performance laptop with 16GB RAM and 512GB SSD storage.",
                            Price = 1200.99m,
                            StockQuantity = 50,
                            StockLocationId = locations[0].Id,
                            StockLocation = locations[0]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Smartphone",
                            ShortDescription = "Latest model smartphone",
                            Description = "Latest model smartphone with 8GB RAM and 128GB storage.",
                            Price = 799.99m,
                            StockQuantity = 150,
                            StockLocationId = locations[1].Id,
                            StockLocation = locations[1]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Tablet",
                            ShortDescription = "10-inch display tablet",
                            Description = "A 10-inch display tablet with 4GB RAM and 64GB storage.",
                            Price = 299.99m,
                            StockQuantity = 100,
                            StockLocationId = locations[2].Id,
                            StockLocation = locations[2]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Headphones",
                            ShortDescription = "Wireless over-ear headphones",
                            Description = "Wireless over-ear headphones with noise cancellation.",
                            Price = 199.99m,
                            StockQuantity = 200,
                            StockLocationId = locations[0].Id,
                            StockLocation = locations[0]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Smartwatch",
                            ShortDescription = "Fitness tracking smartwatch",
                            Description = "A smartwatch with fitness tracking and heart rate monitor.",
                            Price = 149.99m,
                            StockQuantity = 300,
                            StockLocationId = locations[1].Id,
                            StockLocation = locations[1]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Camera",
                            ShortDescription = "DSLR camera",
                            Description = "A DSLR camera with 24.2MP resolution and 18-55mm lens.",
                            Price = 499.99m,
                            StockQuantity = 75,
                            StockLocationId = locations[2].Id,
                            StockLocation = locations[2]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Printer",
                            ShortDescription = "All-in-one printer",
                            Description = "An all-in-one printer with scanning and copying features.",
                            Price = 89.99m,
                            StockQuantity = 120,
                            StockLocationId = locations[0].Id,
                            StockLocation = locations[0]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Monitor",
                            ShortDescription = "27-inch 4K monitor",
                            Description = "A 27-inch 4K monitor with HDR and wide color gamut.",
                            Price = 399.99m,
                            StockQuantity = 60,
                            StockLocationId = locations[1].Id,
                            StockLocation = locations[1]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Keyboard",
                            ShortDescription = "Mechanical keyboard",
                            Description = "A mechanical keyboard with RGB backlighting.",
                            Price = 79.99m,
                            StockQuantity = 250,
                            StockLocationId = locations[2].Id,
                            StockLocation = locations[2]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Mouse",
                            ShortDescription = "Wireless ergonomic mouse",
                            Description = "A wireless ergonomic mouse with programmable buttons.",
                            Price = 49.99m,
                            StockQuantity = 180,
                            StockLocationId = locations[0].Id,
                            StockLocation = locations[0]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Speaker",
                            ShortDescription = "Bluetooth speaker",
                            Description = "A portable Bluetooth speaker with 10-hour battery life.",
                            Price = 59.99m,
                            StockQuantity = 220,
                            StockLocationId = locations[1].Id,
                            StockLocation = locations[1]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "External Hard Drive",
                            ShortDescription = "1TB external hard drive",
                            Description = "A 1TB external hard drive with USB 3.0 interface.",
                            Price = 69.99m,
                            StockQuantity = 140,
                            StockLocationId = locations[2].Id,
                            StockLocation = locations[2]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Router",
                            ShortDescription = "Dual-band Wi-Fi router",
                            Description = "A dual-band Wi-Fi router with high-speed performance.",
                            Price = 129.99m,
                            StockQuantity = 90,
                            StockLocationId = locations[0].Id,
                            StockLocation = locations[0]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Webcam",
                            ShortDescription = "1080p HD webcam",
                            Description = "A 1080p HD webcam with built-in microphone.",
                            Price = 39.99m,
                            StockQuantity = 110,
                            StockLocationId = locations[1].Id,
                            StockLocation = locations[1]
                        },
                        new Product
                        {
                            Id = Guid.NewGuid(),
                            Name = "Charger",
                            ShortDescription = "Fast charging adapter",
                            Description = "A fast charging adapter compatible with multiple devices.",
                            Price = 19.99m,
                            StockQuantity = 500,
                            StockLocationId = locations[2].Id,
                            StockLocation = locations[2]
                        }
                        };
                        context.Products.AddRange(products);
                        context.SaveChanges();
                    }
                }
            }
        }
    }
}