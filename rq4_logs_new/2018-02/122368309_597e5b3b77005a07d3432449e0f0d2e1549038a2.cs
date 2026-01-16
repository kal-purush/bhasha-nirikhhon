using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace ParseTests.Serialization
{
  [TestClass]
  public class SerializationTests
  {
    [TestMethod]
    public void SerializeItem(){
      Item item = new Item{Words = {{"de", "Stadt"},{"en", "city"}}};
      string json  = JsonConvert.SerializeObject(item);
      Assert.AreEqual(@"{""Words"":{""de"":""Stadt"",""en"":""city""}}", json);
    }

    [TestMethod]
    public void DeserializeItem(){
      string json = @"{""Words"":{""de"":""Stadt"",""en"":""city""}}";
      Item item = JsonConvert.DeserializeObject<Item>(json);
      Assert.AreEqual(2, item.Words.Count);
    }

    [TestMethod]
    public void SerializeListOfItem(){
      List<Item> list = new List<Item>{new Item{Words = {{"de", "Stadt"},{"en", "city"}}}, new Item{Words = {{"de", "Straße"},{"en", "street"}}}};
      string json = JsonConvert.SerializeObject(list);
      Assert.IsNotNull(json);
    }


    [TestMethod]
    public void DeserializeListOfItem(){
      string json = @"[{""Words"":{""de"":""Stadt"",""en"":""city""}},{""Words"":{""de"":""Straße"",""en"":""street""}}]";
      List<Item> list = JsonConvert.DeserializeObject<List<Item>>(json);
      Assert.IsNotNull(list);
      Assert.AreEqual(2, list.Count);
    }


    [TestMethod]
    public void SerializeItemCollection(){
      ItemsCollection list = new ItemsCollection{new Item{Words = {{"de", "Stadt"},{"en", "city"}}}, new Item{Words = {{"de", "Straße"},{"en", "street"}}}};
      string json = JsonConvert.SerializeObject(list);
      Assert.IsNotNull(json);
    }

    [TestMethod]
    public void DeserializeItemCollection(){
      string json = @"[{""Words"":{""de"":""Stadt"",""en"":""city""}},{""Words"":{""de"":""Straße"",""en"":""street""}}]";
      ItemsCollection list = JsonConvert.DeserializeObject<ItemsCollection>(json);
      Assert.IsNotNull(list);
      Assert.AreEqual(2, list.Count);
      Assert.AreEqual("city", list["de:Stadt"].Words["en"]);
      Assert.AreEqual("Straße", list["en:street"].Words["de"]);
    }
  }
}