using System;

namespace ClassesRecup
{
    public class Abstract
    {
    }
    // абстрактный класс по сути некая идея, описание "контракта" с возможной частичной реализацией поведения обьекта
    // наследуемого от абстрактного класса
    // свойство - это по сути "метод" для упрощения работы с get и set который маскируется под поле

    abstract class Weapon
    {
        public abstract int Damage { get; }
        // абстрактные методы могут находиться только внутри абстрактных классов
        public abstract void Fire();

        public void ShowInfo()
        {
            Console.WriteLine($"{GetType().Name} Damage: {Damage}"); // выводим имя типа данных в котором мы
        }
    }

    class Gun :Weapon 
    {
        // Наследники абстрактного класса должны реализовать его абстрактные члены(контракт)
        // Таким образом мы гарантируем, что все наследники будут иметь метод 
        // А Player из этого "нечто" стрелять

        public override int Damage { get { return 5; } }

        public override void Fire()
        {
            // override работает и с virtual и с abstract
            Console.WriteLine("Pow!");
        }
    }

    class Bow : Weapon
    {
        public override int Damage => 3; // =>  - лямбда-выражение

        public override void Fire()
        {
            Console.WriteLine("Wdoom");
        }
    }

    class LaserGun : Weapon
    {
        public override int Damage => 6;
        public override void Fire()
        {
            Console.WriteLine("Pew pew");
        }
    }

    class Player
    {
        // у абстрактных классов нельзя создавать экземпляры
        // ссылки базовых классов могут хранить экземпляры наследников
        public void Fire(Weapon weapon) // Игрок сможет стрелять из всего, что унаследованно от класса Weapon - Полиморфизм
        {
            weapon.Fire();
        }

        public void CheckInfo(Weapon weapon)
        {
            weapon.ShowInfo();
        }
    }
}