using System;
using System.Collections.Generic;

namespace StudyExercises
{
    // ����� ������
    public class Address
    {
        public string Street { get; set; }
        public string City { get; set; }
        public string ZipCode { get; set; }

        public override string ToString()
        {
            return $"{Street}, {City}, {ZipCode}";
        }
    }

    // ����������� ����� ��������
    public abstract class Delivery
    {
        public Address Address { get; set; }

        public abstract void PrepareDelivery();
    }

    // �������� �� ���
    public class HomeDelivery : Delivery
    {
        public string CourierName { get; set; }

        public override void PrepareDelivery()
        {
            Console.WriteLine($"���������� �������� �� ��� � �������� {CourierName}");
        }
    }

    // �������� � ����� ������
    public class PickPointDelivery : Delivery
    {
        public string PickupPointName { get; set; }

        public override void PrepareDelivery()
        {
            Console.WriteLine($"�������� � ����� ������ {PickupPointName}");
        }
    }

    // �������� � �������
    public class ShopDelivery : Delivery
    {
        public string ShopName { get; set; }

        public override void PrepareDelivery()
        {
            Console.WriteLine($"�������� � ������� {ShopName}");
        }
    }

    // ����� ��������
    public class Product
    {
        public string Name { get; set; }
        public decimal Price { get; set; }

        public override string ToString()
        {
            return $"{Name} - {Price:C}";
        }
    }

    // ����� � �����������
    public class Order<TDelivery> where TDelivery : Delivery
    {
        private List<Product> products = new List<Product>();

        public TDelivery Delivery { get; set; }
        public int Number { get; set; }
        public string Description { get; set; }

        // ���������� ��� ������� � ���������
        public Product this[int index]
        {
            get => products[index];
            set => products[index] = value;
        }

        public void AddProduct(Product product)
        {
            products.Add(product);
        }

        public void RemoveProduct(Product product)
        {
            products.Remove(product);
        }

        public void DisplayAddress()
        {
            Console.WriteLine(Delivery.Address);
        }

        public decimal CalculateTotalPrice()
        {
            decimal total = 0;
            foreach (var product in products)
            {
                total += product.Price;
            }
            return total;
        }

        public void ShowOrderDetails()
        {
            Console.WriteLine($"����� #{Number}: {Description}");
            Console.WriteLine("������ � ������:");
            foreach (var product in products)
            {
                Console.WriteLine(product);
            }
            Console.WriteLine($"�����: {CalculateTotalPrice():C}");
            DisplayAddress();
        }

        // ���������� ��������� ==
        public static bool operator ==(Order<TDelivery> order1, Order<TDelivery> order2)
        {
            return order1.Number == order2.Number;
        }

        // ���������� ��������� !=
        public static bool operator !=(Order<TDelivery> order1, Order<TDelivery> order2)
        {
            return !(order1 == order2);
        }

        public override bool Equals(object obj)
        {
            if (obj is Order<TDelivery> otherOrder)
            {
                return this == otherOrder;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return Number.GetHashCode();
        }
    }

    // ������ ���������� ��� ������ ���������� � ������
    public static class OrderExtensions
    {
        public static void PrintTotalPrice<TDelivery>(this Order<TDelivery> order) where TDelivery : Delivery
        {
            Console.WriteLine($"����� ��������� ������ #{order.Number}: {order.CalculateTotalPrice():C}");
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var order = new Order<HomeDelivery>
            {
                Number = 1,
                Description = "����� �� �������� ��������",
                Delivery = new HomeDelivery
                {
                    Address = new Address { Street = "������ 20", City = "������", ZipCode = "101000" },
                    CourierName = "����"
                }
            };

            order.AddProduct(new Product { Name = "����", Price = 50 });
            order.AddProduct(new Product { Name = "������", Price = 80 });

            order.ShowOrderDetails();
            order.Delivery.PrepareDelivery();
            // ������������� ������ ����������
            order.PrintTotalPrice();

            // �������� ���������� ����������
            var anotherOrder = new Order<HomeDelivery> { Number = 1 };
            Console.WriteLine(order == anotherOrder ? "������ �����." : "������ �� �����.");

            Console.ReadKey();
        }
    }
}