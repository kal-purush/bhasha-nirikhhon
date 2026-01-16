using System.Collections.Generic;
using System.Linq;
using API.Entities;

namespace API.Data
{
    public static class DbInitializer
    {
        public static void Initialize(StoreContext context)
        {
            if (context.Products.Any()) return;

            var products = new List<Product>
            {
                new Product
                {
                    Name = "Xbox Series X",
                    Description =
                        "The Xbox Series X delivers sensationally smooth frame rates of up to 120FPS with the visual pop of HDR. Immerse yourself with sharper characters, brighter worlds, and impossible details with true-to-life 4K.",
                    Price = 499,
                    PictureUrl = "/images/products/console-xboxsx.png",
                    Brand = "Xbox",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Xbox Series S",
                    Description = "Next-gen performance in the smallest Xbox ever. Go all digital with Xbox Series S and enjoy next-gen gaming at a great price",
                    Price = 300,
                    PictureUrl = "/images/products/console-xboxs.png",
                    Brand = "Xbox",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "PlayStation 5",
                    Description =
                        "Experience lightning-fast loading with an ultra-high speed SSD, deeper immersion with support for haptic feedback, adaptive triggers and 3D Audio",
                    Price = 825,
                    PictureUrl = "/images/products/console-ps5.jpg",
                    Brand = "Sony",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "PlayStation 5 Digital Edition",
                    Description =
                        "PlayStation 5 Digital Edition Console ; Up to 120fps with 120Hz Output - Enjoy smooth and fluid high frame rate gameplay at up to 120fps for compatible games,",
                    Price = 489,
                    PictureUrl = "/images/products/console-ps5d.jpg",
                    Brand = "Nintendo",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Nintendo Switch",
                    Description =
                        "Nintendo Switch is designed to fit your life, transforming from home console to portable system in a snap.",
                    Price = 295,
                    PictureUrl = "/images/products/console-switch.jpg",
                    Brand = "Nintendo",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Nintendo Switch OLED",
                    Description =
                        "The new system features a vibrant 7-inch OLED screen, a wide adjustable stand, a dock with a wired LAN port, 64 GB of internal storage, and enhanced audio.",
                    Price = 495,
                    PictureUrl = "/images/products/console-switcholed.jpg",
                    Brand = "Nintendo",
                    Type = "Console",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Xbox Wireless Controller",
                    Description =
                        "Xbox Wireless Controller ; Switch devices. Easily pair and switch between devices including Xbox Series X, Xbox Series S, Xbox One, Windows PC, Android, and iOS.",
                    Price = 65,
                    PictureUrl = "/images/products/controller-xbox.jpg",
                    Brand = "Xbox",
                    Type = "Controller",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "PlayStation 5 Wireless Controller",
                    Description =
                        "The DualSense wireless controller for PS5 offers immersive haptic feedback2, dynamic adaptive triggers and a built-in microphone, all integrated into an iconic design.",
                    Price = 70,
                    PictureUrl = "/images/products/conrtoller-ps5.jpg",
                    Brand = "Sony",
                    Type = "Controller",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Nintendo 64 Switch Controller",
                    Description =
                        "This controller is the perfect way to explore 3D worlds that debuted on the N64 system. It even includes built-in rumble functionality for compatible games",
                    Price = 80,
                    PictureUrl = "/images/products/controller-switch.jpg",
                    Brand = "Nintendo",
                    Type = "Controller",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Google Pixel 6 128GB",
                    Description =
                        "Introducing Pixel 6 Pro, the completely redesigned, fully loaded Google 5G phone.* With a powerful camera system, next-gen security, and the custom-built Google Tensor processor, it's the smartest and fastest Pixel yet.",
                    Price = 749,
                    PictureUrl = "/images/products/phone-pixel6.jpg",
                    Brand = "Google",
                    Type = "Phones",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Google Pixel 6 Pro 256GB",
                    Description =
                        "Introducing Pixel 6 Pro, the completely redesigned, fully loaded Google 5G phone.* With a powerful camera system, next-gen security, and the custom-built Google Tensor processor, it's the smartest and fastest Pixel yet.",
                    Price = 849,
                    PictureUrl = "/images/products/gphone-pixel6pro.png",
                    Brand = "Google",
                    Type = "Phones",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Oculus Quest 2 64GB",
                    Description =
                        "Advanced All-in-One Virtual Reality Gaming Headset",
                    Price = 399,
                    PictureUrl = "/images/products/vr-quest64gb.jpg",
                    Brand = "Oculus",
                    Type = "VR",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Oculus Quest 2 256GB",
                    Description =
                        "Advanced All-in-One Virtual Reality Gaming Headset",
                    Price = 800,
                    PictureUrl = "/images/products/vr-quest256gb.jpg",
                    Brand = "Oculus",
                    Type = "VR",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "EVGA GeForce RTX 3090",
                    Description =
                        "The GeForce RTX™ 3090 Ti and RTX 3090 let you take on the latest games using the power of Ampere—NVIDIA's 2nd generation RTX architecture.",
                    Price = 1099,
                    PictureUrl = "/images/products/gpu-evga3090.jpg",
                    Brand = "EVGA",
                    Type = "GPU",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Gigabyte GeForce RTX 3090",
                    Description =
                        "The GeForce RTX™ 3090 Ti and RTX 3090 let you take on the latest games using the power of Ampere—NVIDIA's 2nd generation RTX architecture.",
                    Price = 1299,
                    PictureUrl = "/images/products/gpu-gigabyte3090.jpg",
                    Brand = "Gigabyte",
                    Type = "GPU",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Asus GeForce RTX 3090",
                    Description =
                        "The GeForce RTX™ 3090 Ti and RTX 3090 let you take on the latest games using the power of Ampere—NVIDIA's 2nd generation RTX architecture.",
                    Price = 1200,
                    PictureUrl = "/images/products/gpu-asus3090.jpg",
                    Brand = "Asus",
                    Type = "GPU",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "Asus GeForce RTX 3060",
                    Description = "The GeForce RTX™ 3060 Ti and RTX 3060 let you take on the latest games using the power of Ampere—NVIDIA's 2nd generation RTX architecture.",
                    Price = 475,
                    PictureUrl = "/images/products/gpu-asus3060.jpg",
                    Brand = "Asus",
                    Type = "GPU",
                    QuantityInStock = 100
                },
                new Product
                {
                    Name = "LG UltraGear Gaming Monitor",
                    Description =
                        "Complete your battle station with a premium LG UltraGear™ Gaming Monitor. Built for gamers, it delivers the latest hardware, specs, ergonomics, sleek design and sensory experience",
                    Price = 899,
                    PictureUrl = "/images/products/monitor-lg.jpg",
                    Brand = "LG",
                    Type = "Monitor",
                    QuantityInStock = 100
                },
            };

            foreach (var product in products)
            {
                context.Products.Add(product);
            }

            context.SaveChanges();
        }

    }
}