using UnityEngine;
using UnityEditor;
using UnityEngine.TestTools;
using NUnit.Framework;
using System.Collections;
using System.Collections.Generic;

public class Test {
    public static List<string> list1 = new List<string>();
    
    string[] a = { "0", "nihao", "123", "1", "as", "asd" };
    

    [Test]
	public void exitsName() {
        // Use the Assert class to test conditions.
        list1.AddRange(a);
        Assert.AreEqual(true, Reigster.exist("nihao",list1));
        Assert.AreEqual(true, Reigster.exist("as", list1));
        Assert.AreEqual(false, Reigster.exist("asd", list1));
        Assert.AreEqual(false, Reigster.exist("123", list1));
    }
    [Test]
    public void rightPassword()
    {
        list1.AddRange(a);
        // Use the Assert class to test conditions.
        Assert.AreEqual(true, Login.check("nihao","123", list1));
        Assert.AreEqual(false, Login.check("asd","1", list1));
    }

    [Test]
    public void register()
    {
        Assert.AreEqual(true, Reigster.inputData(0, "nihao", "123"));
        // Use the Assert class to test conditions.


    }
}