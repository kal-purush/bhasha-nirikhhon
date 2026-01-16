using System;

namespace Exercise7
{
    public class Dog
    {
        public string name;
        public string sex;
        private Dog _mother;
        private Dog _father;
        public bool HasMother=false;
        public bool HasFather = false;

        public Dog(string name, string sex)
        {
            this.name = name;
            this.sex = sex;           
        }

        public Dog GetFather()
        {
            if (this.HasFather)
            {
                return this._father;
            }
            else return null;
        }

        public string GetFatherName()
        {
            if (this.HasFather)
            {
                return this._father.name;
            }
            else return "Unknown";
        }

        public Dog GetMother()
        {
            if (this.HasMother)
            {
                return this._mother;
            }
            else return null;
        }

        public string GetMotherName()
        {
            if (this.HasMother)
            {
                return this._mother.name;
            }
            else return "Unknown";
        }

        public void SetFather(Dog father)
        {
            this._father = father;
            this.HasFather = true;
        }

        public void SetMother(Dog mother)
        {
            this._mother = mother;
            this.HasMother = true;
        }

        public bool HasSameMotherAs(Dog dog)
        {
            return (this.GetMotherName() == dog.GetMotherName());
        }
    }
}