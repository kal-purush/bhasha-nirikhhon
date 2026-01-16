using IField;
using McDroid;

Sheep sheep = new Sheep();
IField.Pig pig1 = new IField.Pig();

Cow cow = new Cow();
McDroid.Pig pig2 = new McDroid.Pig();

namespace IField
{
    public class Sheep { }
    public class Pig { }
}

namespace McDroid
{
    public class Cow { }
    public class Pig { }
}