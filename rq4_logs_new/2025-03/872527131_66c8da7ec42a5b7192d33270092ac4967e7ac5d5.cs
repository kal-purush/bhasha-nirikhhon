using System;

namespace TondForooshApi.Models;

public class SeedData
{
    private TFDbContext context;

    public SeedData(TFDbContext ctx)
    {
        context = ctx;
    }
    public void Initialize()
    {
        // Check if the Products table has any data
        if (context.Products.Any())
        {
            return;   // Data is already seeded
        }

        context.Products.AddRange(
            new Product
            {
                Name = "لپ‌تاپ",
                Description = "لپ‌تاپ با عملکرد بالا برای کار و بازی.",
                Price = 999.99m,
                ImageUrl = "https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.senetic.es%2Fproduct%2FLGI-00042&psig=AOvVaw0BRRxtiai_54IbG8JId9Qw&ust=1741288319436000&source=images&cd=vfe&opi=89978449&ved=0CBMQjRxqFwoTCMCWtuDS84sDFQAAAAAdAAAAABAE"
            },
            new Product
            {
                Name = "گوشی هوشمند",
                Description = "گوشی هوشمند با دوربین عالی و ویژگی‌های جدید.",
                Price = 699.99m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/c23b49b0be1c4ae5b2a3d7a3281d2f1731065243_1726037574.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "هدفون",
                Description = "هدفون‌های عایق صدا برای تجربه بهتر موسیقی.",
                Price = 199.99m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/622d3e84ed7a8480c5320a3d085f6735c86e5ddf_1653922440.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "تلویزیون هوشمند",
                Description = "تلویزیون با کیفیت بالا و قابلیت اتصال به اینترنت.",
                Price = 450.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/02f0e34a248303807e175d1cd7e61dc7d50782c5_1708417372.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "دوربین دیجیتال",
                Description = "دوربین دیجیتال با رزولوشن بالا برای عکاسی حرفه‌ای.",
                Price = 750.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/112439699.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "کامپیوتر رومیزی",
                Description = "سیستم قدرتمند برای گیمینگ و کارهای حرفه‌ای.",
                Price = 1200.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/f95b0a290e59c0867946eec51e6ada63040e9677_1658915605.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "ساعت هوشمند",
                Description = "ساعت هوشمند با قابلیت‌های تناسب اندام و نظارت بر سلامت.",
                Price = 150.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/165e6d0b58e43496172339684059ce611d274057_1736070009.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "کیف لپ‌تاپ",
                Description = "کیف مقاوم و زیبا برای حمل لپ‌تاپ.",
                Price = 40.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/253afe2fe82c733c22a614d2b04d4ec42f43ce3f_1716906040.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "مینی یخچال",
                Description = "یخچال کوچک برای دفاتر کار و اتاق‌های خواب.",
                Price = 180.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/c75c59c442e0560cef05a463b2dc57d9fd76c4a4_1713187990.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "پرینتر رنگی",
                Description = "پرینتر رنگی با کیفیت بالا برای چاپ اسناد و تصاویر.",
                Price = 220.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/1381342.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "دستگاه تصفیه هوا",
                Description = "دستگاه تصفیه هوا با فیلتر HEPA برای محیطی سالم.",
                Price = 100.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/467af9d48e8face9842086ad325dc328671fa39a_1740487343.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "کنسول بازی",
                Description = "کنسول بازی با قابلیت پشتیبانی از بازی‌های 4K.",
                Price = 500.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/586904991b9ca0e31eeaf9e2953e9d509cc10af5_1692696626.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "دوربین مداربسته",
                Description = "دوربین امنیتی با کیفیت تصویر 1080p و قابلیت اتصال به اینترنت.",
                Price = 80.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/f291ac0a21bce334b1bf7e3a262130f6dcbd621c_1669612093.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            },
            new Product
            {
                Name = "یخچال فریزر",
                Description = "یخچال فریزر بزرگ با طراحی مدرن و مصرف انرژی کم.",
                Price = 850.00m,
                ImageUrl = "https://dkstatics-public.digikala.com/digikala-products/5a22130bfdf9a0548ee6356bce02d73578546863_1687610173.jpg?x-oss-process=image/resize,m_lfit,h_800,w_800/format,webp/quality,q_90"
            }
        );

        context.SaveChanges();  // Persist changes to the database
    }
}