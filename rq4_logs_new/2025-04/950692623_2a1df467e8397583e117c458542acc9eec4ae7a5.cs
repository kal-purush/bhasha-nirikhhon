using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace HotelReservation.Utils
{
    public static class DataGenerator
    {
        private static readonly Random _rnd = new Random();

        // Regex Patterns
        private static readonly string EmailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
        private static readonly string PhonePattern = @"^\+?[1-9]\d{1,14}$";

        // Dữ liệu tên và email
        private static readonly List<string> FirstNames = new() { "Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Do", "Mai", "Ngo", "Bui" };
        private static readonly List<string> MiddleNames = new() { "Van", "Thi", "Quang", "Thanh", "Duc", "Ngoc", "Minh", "Hoang", "Anh", "Thao" };
        private static readonly List<string> LastNames = new() { "An", "Bao", "Cuong", "Duy", "Oanh", "Van", "Long", "Hieu", "Dang", "Kien" };
        private static readonly List<string> EmailProviders = new()
        {
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"
        };

        // -------------------------------
        // 1. Generate Account
        // -------------------------------
        public static Account GenerateRandomAccount(uint id, bool isValid)
        {
            string first = FirstNames[_rnd.Next(FirstNames.Count)];
            string middle = MiddleNames[_rnd.Next(MiddleNames.Count)];
            string last = LastNames[_rnd.Next(LastNames.Count)];

            string provider = EmailProviders[_rnd.Next(EmailProviders.Count)];
            string localPart = $"{last}.{first}".ToLower() + _rnd.Next(1, 1000) + _rnd.Next(1, 1000);
            string email = $"{localPart}@{provider}";

            // Email không hợp lệ
            if (!isValid)
            {
                if (_rnd.NextDouble() < 0.3)
                    email = $"{localPart}{provider}";
                else if (_rnd.NextDouble() < 0.6)
                    email = $"{localPart}@";
                else if (_rnd.NextDouble() < 0.8)
                    email = $"{localPart}@@{provider}";
                else
                    email = $"@{provider}";
            }

            // Tạo số điện thoại
            string phone = "";
            for (int i = 0; i < 10; i++)
                phone += _rnd.Next(1, 10);
            if (_rnd.NextDouble() < 0.5)
                phone = "+" + phone;

            // Số điện thoại không hợp lệ
            if (!isValid)
            {
                if (_rnd.NextDouble() < 0.3)
                    phone = _rnd.Next(1, 10).ToString();
                else if (_rnd.NextDouble() < 0.6)
                    phone = _rnd.Next(999, 1000000).ToString() + "12345678910119";
                else if (_rnd.NextDouble() < 0.8)
                    phone = _rnd.Next(1, 100) + ((char)_rnd.Next('a', 'z' + 1)).ToString() + _rnd.Next(1, 10000);
                else
                    phone = _rnd.Next(10, 100000000) + "#";

                if (_rnd.NextDouble() < 0.5)
                    phone = "+" + phone;
            }

            // Mật khẩu ngẫu nhiên
            string pwdChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*";
            int pwdLen = _rnd.Next(8, 13);
            char[] pwd = new char[pwdLen];
            for (int i = 0; i < pwdLen; i++)
                pwd[i] = pwdChars[_rnd.Next(pwdChars.Length)];
            string password = new string(pwd);

            // Kiểm tra hợp lệ
            bool isEmailValid = Regex.IsMatch(email, EmailPattern);
            bool isPhoneValid = Regex.IsMatch(phone, PhonePattern);

            if (isValid)
            {
                while (!isEmailValid || !isPhoneValid)
                {
                    email = $"{localPart}@{provider}";
                    phone = "0";
                    for (int i = 0; i < 9; i++)
                        phone += _rnd.Next(1, 10);
                    isEmailValid = Regex.IsMatch(email, EmailPattern);
                    isPhoneValid = Regex.IsMatch(phone, PhonePattern);
                }
            }

            return new Account
            {
                ID = id,
                Name = $"{first} {middle} {last}",
                Email = email,
                Phone = phone,
                Password = password
            };
        }

        public static List<Account> GenerateAccountsInRange(uint startId, uint endId, bool isValid)
        {
            var accounts = new List<Account>();
            for (uint id = startId; id <= endId; id++)
            {
                var acc = GenerateRandomAccount(id, isValid);
                accounts.Add(acc);
            }
            return accounts;
        }

        // -------------------------------
        // 2. Generate Room Description
        // -------------------------------
        public static RoomDescription GenerateRandomRoomDescription()
        {
            var maxOccupancy = (ushort)_rnd.Next(1, 6);
            ushort numberOfBeds;
            BedSize bedSize;

            if (maxOccupancy == 1)
            {
                numberOfBeds = (ushort)_rnd.Next(1, 3);
                bedSize = (BedSize)_rnd.Next(0, 3);
            }
            else if (maxOccupancy == 2 || maxOccupancy == 3)
            {
                numberOfBeds = (ushort)_rnd.Next(1, 3);
                bedSize = (BedSize)_rnd.Next(3, 5);
            }
            else if (maxOccupancy == 4)
            {
                numberOfBeds = 2;
                bedSize = (BedSize)_rnd.Next(3, 6);
            }
            else
            {
                numberOfBeds = 2;
                bedSize = (BedSize)_rnd.Next(5, 7);
            }

            return new RoomDescription
            {
                Property = new RoomProperty
                {
                    MaxOccupancy = maxOccupancy,
                    NumberOfBeds = numberOfBeds,
                    BedType = new BedType
                    {
                        Size = bedSize,
                        Design = (BedDesign)_rnd.Next(Enum.GetValues<BedDesign>().Length),
                        Style = (BedStyle)_rnd.Next(Enum.GetValues<BedStyle>().Length),
                        KidType = (BedKid)_rnd.Next(Enum.GetValues<BedKid>().Length),
                        Frame = (BedFrame)_rnd.Next(Enum.GetValues<BedFrame>().Length)
                    },
                    ViewType = (ViewType)_rnd.Next(Enum.GetValues<ViewType>().Length)
                },
                Amenity = new RoomAmenity
                {
                    HasWifi = _rnd.NextDouble() < 0.9,
                    HasTV = _rnd.NextDouble() < 0.8,
                    HasBalcony = _rnd.NextDouble() < 0.5,
                    HasKitchen = _rnd.NextDouble() < 0.3,
                    HasBathTub = _rnd.NextDouble() < 0.6,
                    IsSmokingAllowed = _rnd.NextDouble() < 0.2
                },
                Restriction = new RoomRestriction
                {
                    Refundable = _rnd.NextDouble() < 0.4,
                    MinStay = (ushort)_rnd.Next(1, 3),
                    MaxStay = (ushort)_rnd.Next(3, 15)
                },
                Accessibility = new RoomAccessibility
                {
                    IsWheelchairFriendly = _rnd.NextDouble() < 0.3,
                    HasBrailleSignage = _rnd.NextDouble() < 0.2
                }
            };
        }

        public static void PrintRoomDescription(RoomDescription roomDescription)
        {
            Console.WriteLine("Room Description:");
            Console.WriteLine($"Max Occupancy: {roomDescription.Property.MaxOccupancy}");
            Console.WriteLine($"Number of Beds: {roomDescription.Property.NumberOfBeds}");
            Console.WriteLine($"Bed Size: {roomDescription.Property.BedType.Size}");
            Console.WriteLine($"Bed Design: {roomDescription.Property.BedType.Design}");
            Console.WriteLine($"Bed Style: {roomDescription.Property.BedType.Style}");
            Console.WriteLine($"Bed Kid Type: {roomDescription.Property.BedType.KidType}");
            Console.WriteLine($"Bed Frame: {roomDescription.Property.BedType.Frame}");
            Console.WriteLine($"View Type: {roomDescription.Property.ViewType}");

            Console.WriteLine("Amenities:");
            Console.WriteLine($"Has Wifi: {roomDescription.Amenity.HasWifi}");
            Console.WriteLine($"Has TV: {roomDescription.Amenity.HasTV}");
            Console.WriteLine($"Has Balcony: {roomDescription.Amenity.HasBalcony}");
            Console.WriteLine($"Has Kitchen: {roomDescription.Amenity.HasKitchen}");
            Console.WriteLine($"Has BathTub: {roomDescription.Amenity.HasBathTub}");
            Console.WriteLine($"Is Smoking Allowed: {roomDescription.Amenity.IsSmokingAllowed}");

            Console.WriteLine("Restrictions:");
            Console.WriteLine($"Refundable: {roomDescription.Restriction.Refundable}");
            Console.WriteLine($"Min Stay: {roomDescription.Restriction.MinStay}");
            Console.WriteLine($"Max Stay: {roomDescription.Restriction.MaxStay}");

            Console.WriteLine("Accessibility:");
            Console.WriteLine($"Wheelchair Friendly: {roomDescription.Accessibility.IsWheelchairFriendly}");
            Console.WriteLine($"Has Braille Signage: {roomDescription.Accessibility.HasBrailleSignage}");
        }
    }
}