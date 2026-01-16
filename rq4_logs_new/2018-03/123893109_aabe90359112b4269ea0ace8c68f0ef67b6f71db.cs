using Newtonsoft.Json;

namespace DODB2_2.Model
{

    public class Person
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        [JsonProperty(PropertyName = "telephoneNumbers")]
        public Telephonenumber[] TelephoneNumbers { get; set; }
        [JsonProperty(PropertyName = "primaryAdress")]
        public Primaryadress PrimaryAdress { get; set; }
        [JsonProperty(PropertyName = "secondaryAdress")]
        public Secondaryadress[] SecondaryAdress { get; set; }
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class Primaryadress
    {
        [JsonProperty(PropertyName = "adressname")]
        public string Adressname { get; set; }
        [JsonProperty(PropertyName = "city")]
        public City City { get; set; }
    }

    public class Telephonenumber
    {
        [JsonProperty(PropertyName = "number")]
        public string Number { get; set; }
        [JsonProperty(PropertyName = "provider")]
        public string Provider { get; set; }
    }

    public class Secondaryadress
    {
        [JsonProperty(PropertyName = "adressname")]
        public string Adressname { get; set; }
        [JsonProperty(PropertyName = "adressType")]
        public string AdressType { get; set; }
        [JsonProperty(PropertyName = "city")]
        public City City { get; set; }
    }

    public class City
    {
        [JsonProperty(PropertyName = "name")]
        public string Name { get; set; }
        [JsonProperty(PropertyName = "zipCode")]
        public Zipcode ZipCode { get; set; }
    }

    public class Zipcode
    {
        [JsonProperty(PropertyName = "code")]
        public string Code { get; set; }
        [JsonProperty(PropertyName = "countryCode")]
        public string CountryCode { get; set; }
    }

}