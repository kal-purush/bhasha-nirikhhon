
// // ürün listesi
// Console.WriteLine("---------------------------");
// Console.WriteLine("Rüya Manavına HOŞGELDİNİZ..");
// Console.WriteLine("Elma: 2 TL\nArmut: 3 TL\nÇilek: 2 TL\nMuz: 3 TL\nDiğer bütün meyveler: 4 TL");
// Console.WriteLine("---------------------------");
// Console.Write("Lütfen hangi ürünü istediğinizi giriniz: ");

// // kullanıcının girdiği büyük küçük harf hassasiyetini kaldırmak için düzeltme yapılıyor
// string? secim = Console.ReadLine().ToLower(); // kullanıcını girdiği türkçe karakterlerde sorun oluşabilir..

// // if else ile ürün seçimi
// if (secim == "elma")
// {
//     Console.WriteLine("Seçtiğiniz meyvenin fiyatı 2 TL");
// }
// else if (secim == "armut")
// {
//     Console.WriteLine("Seçtiğiniz meyvenin fiyatı 3 TL");
// }
// else if (secim == "çilek")
// {
//     Console.WriteLine("Seçtiğiniz meyvenin fiyatı 2 TL");
// }
// else if (secim == "muz")
// {
//     Console.WriteLine("Seçtiğiniz meyvenin fiyatı 3 TL");
// }
// else // kullanıcını istediği farklı ürünlerin fiyatını verir
// {
//     Console.WriteLine($"{secim} fiyatı 4 TL");
// }



/* 
Bu uygulamamnın Switch case ile yapılması daha uygun 
çünkü kullanıcıdan alınan değer if else yapısında tek tek dolaşacak ancak switch case 
yapısında bulunduğu satıra gidip yapıdan çıkacak yani çok veri olan durumlarda switch case yapısı kullnımı daha anlamlı olur...
*/

// ürün listesi
Console.WriteLine("---------------------------");
Console.WriteLine("Rüya Manavına HOŞGELDİNİZ..");
Console.WriteLine("Elma: 2 TL\nArmut: 3 TL\nÇilek: 2 TL\nMuz: 3 TL\nDiğer bütün meyveler: 4 TL");
Console.WriteLine("---------------------------");
Console.Write("Lütfen hangi ürünü istediğinizi giriniz: ");

// kullanıcının girdiği büyük küçük harf hassasiyetini kaldırmak için düzeltme yapılıyor
string? secim2 = Console.ReadLine().ToLower(); // kullanıcını girdiği türkçe karakterlerde sorun oluşa bilir..

// Switch Case ile ürün seçimi
switch (secim2)
{
    case "elma":
        Console.WriteLine("Seçtiğiniz meyvenin fiyatı 2 TL");
        break;
    case "armut":
        Console.WriteLine("Seçtiğiniz meyvenin fiyatı 3 TL");
        break;
    case "çilek":
        Console.WriteLine("Seçtiğiniz meyvenin fiyatı 2 TL");
        break;
    case "muz":
        Console.WriteLine("Seçtiğiniz meyvenin fiyatı 3 TL");
        break;
    default: // kullanıcını istediği farklı ürünlerin fiyatını verir
        Console.WriteLine($"{secim2} fiyatı 4 TL");
        break;
}
