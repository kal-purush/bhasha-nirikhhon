using System;
using System.Runtime.Serialization;

namespace DEV_10
{
    /// <summary>
    /// Contains addresses data.
    /// </summary>
    [DataContract]
    [Serializable]
    public class Address : IShopEntity
    {
        /// <summary>
        /// ID of the product.
        /// </summary>
        private int id;

        /// <summary>
        /// Name of the city.
        /// </summary>
        private string city;

        /// <summary>
        /// Name of the street.
        /// </summary>
        private string street;

        /// <summary>
        /// House number.
        /// </summary>
        private int house;

        /// <summary>
        /// Name of the country.
        /// </summary>
        private string country;

        /// <summary>
        /// Gets or sets the ID.
        /// </summary>
        [DataMember]
        public int Id
        {
            get => this.id;
            set
            {
                if (this.id != value)
                {
                    this.id = value;
                    this.EntityChanged?.Invoke(this);
                }
            }
        }

        /// <summary>
        /// Gets or sets the city.
        /// </summary>
        [DataMember]
        public string City
        {
            get => this.city;
            set
            {
                if (this.city != value)
                {
                    this.city = value;
                    this.EntityChanged?.Invoke(this);
                }
            }
        }

        /// <summary>
        /// Gets or sets the street.
        /// </summary>
        [DataMember]
        public string Street
        {
            get => this.street;
            set
            {
                if (this.street != value)
                {
                    this.street = value;
                    this.EntityChanged?.Invoke(this);
                }
            }
        }

        /// <summary>
        /// Gets or sets the house.
        /// </summary>
        [DataMember]
        public int House
        {
            get => this.house;
            set
            {
                if (this.house != value)
                {
                    this.house = value;
                    this.EntityChanged?.Invoke(this);
                }
            }
        }

        /// <summary>
        /// Gets or sets the country.
        /// </summary>
        [DataMember]
        public string Country
        {
            get => this.country;
            set
            {
                if (this.country != value)
                {
                    this.country = value;
                    this.EntityChanged?.Invoke(this);
                }
            }
        }

        /// <inheritdoc />
        public event Action<IShopEntity> EntityChanged;

        /// <inheritdoc />
        public void ShowInfoToConsole()
        {
            Console.WriteLine($"ID {this.Id}: {this.Country}, {this.City}, {this.Street}, {this.House}\n");
        }
    }
}