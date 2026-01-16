class MooClass {
  constructor() {
    console.log("this is me .. bleeding ...", this);
  }
}

var x = new MooClass();
var qt = new Testing.TestModule();
var s1 = Singly.getInstance();
var s2 = Singly.getInstance();

var jozsi =  new People.Person("Jozsi Bacsi");
var matyi = new People.Person("Matyi");
var mike = new People.Driver("Michael Schumacher", "Ferrari");

// Silly tests ...
qt.eq("true should be true", true, true);
qt.eq("`x` should be a `Moo`", x instanceof MooClass, true);
qt.eq("Singleton test `s1`==`s2`", s1==s2, true);

// People tests ...
qt.eq("Fullname should be `Jozsi Bacsi`", jozsi.name, "Jozsi Bacsi");
qt.eq("First name should be `Jozsi`", jozsi.firstName, "Jozsi");
qt.eq("Last name should be `Bacsi`", jozsi.lastName, "Bacsi");
qt.eq("First name should be `Matyi`", matyi.firstName, "Matyi");
qt.eq("Last name should be ``", matyi.lastName, "");

qt.eq("First name should be `Michael`", mike.firstName, "Michael");
qt.eq("Car should be `Ferrari`", mike.car, "Ferrari");
qt.eq("Car should say `brrr...`", mike.drive(), "Ferrari says brrr...");
mike.car = "";
qt.eq("Car should way `walking`", mike.drive(), "walking");

// Util tests ...
qt.eq("[] should be true", Util.ObjectHelper.isArray([]), true);
qt.eq("[1,2,'aaa'] should be true", Util.ObjectHelper.isArray([1,2,"aaa"]), true);
qt.eq("{} should be false", Util.ObjectHelper.isArray({}), false);
qt.eq("`pityu lakatos` -> `Pityu Lakatos`", Util.StringHelper.toTitleCase("pityu lakatos"), "Pityu Lakatos");
qt.eq("`This Test Sentence!` -> `this-test-sentence`", Util.StringHelper.snakify("This Test Sentence!"), "this-test-sentence");