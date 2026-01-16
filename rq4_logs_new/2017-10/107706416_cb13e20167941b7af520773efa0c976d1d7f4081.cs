using System;
using Xunit;

namespace Flagrant.Test
{
    public enum TestEnum
    {
        One, Two, Three
    }
    
    public class FlagrantTests
    {
        [Fact]
        public void Parse_returns_Flagrant_instance()
        {
            Assert.IsType<Flagrant>(Flagrant.Parse(new string[0]));
        }

        // String
        [Fact]
        public void String_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.String("a", "b", out var c));
        }

        [Fact]
        public void String_parses_value()
        {
            var arguments = new[] {"--arg", "value"};
            Flagrant.Parse(arguments)
                .String("arg", "default", out var result);
            Assert.Equal("value", result);
        }

        [Fact]
        public void String_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .String("arg", "default", out var result);
            Assert.Equal("default", result);
        }

        [Fact]
        public void String_defaults_if_no_value()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .String("arg", "default", out var result);
            Assert.Equal("default", result);
        }

        [Fact]
        public void String_defaults_if_next_argument_is_flag()
        {
            var arguments = new[] {"--arg", "--arg2", "value"};
            Flagrant.Parse(arguments)
                .String("arg", "default", out var result);
            Assert.Equal("default", result);
        }
        
        // Int
        [Fact]
        public void Int_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Int("a", 1, out var c));
        }

        [Fact]
        public void Int_parses_value()
        {
            var arguments = new[] {"--arg", "42"};
            Flagrant.Parse(arguments)
                .Int("arg", 21, out var result);
            Assert.Equal(42, result);
        }

        [Fact]
        public void Int_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Int("arg", 21, out var result);
            Assert.Equal(21, result);
        }

        [Fact]
        public void Int_defaults_on_noninteger_value()
        {
            var arguments = new[] {"--arg", "not an int"};
            Flagrant.Parse(arguments)
                .Int("arg", 21, out var result);
            Assert.Equal(21, result);
        }

        [Fact]
        public void Int_defaults_if_next_arg_is_flag()
        {
            var arguments = new[] {"--arg", "--arg2", "42"};
            Flagrant.Parse(arguments)
                .Int("arg", 21, out var result);
            Assert.Equal(21, result);
        }

        [Fact]
        public void Int_defaults_on_no_value()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Int("arg", 21, out var result);
            Assert.Equal(21, result);
        }
        
        // Long
        [Fact]
        public void Long_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Long("a", 1L, out var c));
        }

        [Fact]
        public void Long_parses_value()
        {
            var arguments = new[] {"--arg", "42"};
            Flagrant.Parse(arguments)
                .Long("arg", 21L, out var result);
            Assert.Equal(42L, result);
        }

        [Fact]
        public void Long_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Long("arg", 21L, out var result);
            Assert.Equal(21L, result);
        }

        [Fact]
        public void Long_defaults_on_noninteger_value()
        {
            var arguments = new[] {"--arg", "not a long"};
            Flagrant.Parse(arguments)
                .Long("arg", 21L, out var result);
            Assert.Equal(21L, result);
        }

        [Fact]
        public void Long_defaults_if_next_arg_is_flag()
        {
            var arguments = new[] {"--arg", "--arg2", "42"};
            Flagrant.Parse(arguments)
                .Long("arg", 21L, out var result);
            Assert.Equal(21L, result);
        }

        [Fact]
        public void Long_defaults_on_no_value()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Long("arg", 21L, out var result);
            Assert.Equal(21L, result);
        }
        
        // Float
        [Fact]
        public void Float_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Float("a", 1.0f, out var c));
        }

        [Fact]
        public void Float_parses_value()
        {
            var arguments = new[] {"--arg", "42.5"};
            Flagrant.Parse(arguments)
                .Float("arg", 21.5f, out var result);
            Assert.Equal(42.5f, result);
        }

        [Fact]
        public void Float_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Float("arg", 21.0f, out var result);
            Assert.Equal(21.0f, result);
        }

        [Fact]
        public void Float_defaults_on_noninteger_value()
        {
            var arguments = new[] {"--arg", "not a float"};
            Flagrant.Parse(arguments)
                .Float("arg", 21.5f, out var result);
            Assert.Equal(21.5f, result);
        }

        [Fact]
        public void Float_defaults_if_next_arg_is_flag()
        {
            var arguments = new[] {"--arg", "--arg2", "42.5"};
            Flagrant.Parse(arguments)
                .Float("arg", 21.5f, out var result);
            Assert.Equal(21.5f, result);
        }

        [Fact]
        public void Float_defaults_on_no_value()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Float("arg", 21.5f, out var result);
            Assert.Equal(21.5f, result);
        }
        
        // Double
        [Fact]
        public void Double_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Double("a", 1.0, out var c));
        }

        [Fact]
        public void Double_parses_value()
        {
            var arguments = new[] {"--arg", "42.5"};
            Flagrant.Parse(arguments)
                .Double("arg", 21.5, out var result);
            Assert.Equal(42.5, result);
        }

        [Fact]
        public void Double_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Double("arg", 21.0, out var result);
            Assert.Equal(21.0, result);
        }

        [Fact]
        public void Double_defaults_on_noninteger_value()
        {
            var arguments = new[] {"--arg", "not a float"};
            Flagrant.Parse(arguments)
                .Double("arg", 21.5, out var result);
            Assert.Equal(21.5, result);
        }

        [Fact]
        public void Double_defaults_if_next_arg_is_flag()
        {
            var arguments = new[] {"--arg", "--arg2", "42.5"};
            Flagrant.Parse(arguments)
                .Double("arg", 21.5, out var result);
            Assert.Equal(21.5, result);
        }

        [Fact]
        public void Double_defaults_on_no_value()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Double("arg", 21.5, out var result);
            Assert.Equal(21.5, result);
        }
        
        // Bool
        [Fact]
        public void Bool_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Bool("arg", true, out var c));
        }

        [Fact]
        public void Bool_given_only_argument_parses_as_true()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Bool("arg", false, out var result);
            Assert.True(result);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void Bool_defaults_on_no_argument(bool defaultValue)
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Bool("arg", defaultValue, out var result);
            Assert.Equal(defaultValue, result);
        }

        [Theory]
        [InlineData("false", false)]
        [InlineData("FALSE", true)]
        [InlineData("helloworld", true)]
        public void Bool_true_can_only_be_overwritten_with_lc_false(string value, bool expected)
        {
            var arguments = new[] {$"--arg={value}"};
            Flagrant.Parse(arguments)
                .Bool("arg", true, out var result);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void Bool_true_with_argument_parses_as_true()
        {
            var arguments = new[] {"--arg"};
            Flagrant.Parse(arguments)
                .Bool("arg", true, out var result);
            Assert.True(result);
        }
        
        // Enum
        [Fact]
        public void Enum_returns_Flagrant_instance()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Enum("arg", TestEnum.One, out var result));
        }

        [Fact]
        public void Enum_parses_value()
        {
            var arguments = new[] {"--arg", "One"};
            Flagrant.Parse(arguments)
                .Enum("arg", TestEnum.Two, out var result);
            Assert.Equal(TestEnum.One, result);
        }

        [Fact]
        public void Enum_parses_value_case_insensitive()
        {
            var arguments = new[] {"--arg", "tWo"};
            Flagrant.Parse(arguments)
                .Enum("arg", TestEnum.One, out var result);
            Assert.Equal(TestEnum.Two, result);
        }

        [Fact]
        public void Enum_defaults_on_no_argument()
        {
            var arguments = new string[0];
            Flagrant.Parse(arguments)
                .Enum("arg", TestEnum.Two, out var result);
            Assert.Equal(TestEnum.Two, result);
        }

        [Fact]
        public void Enum_defaults_with_invalid_value()
        {
            var arguments = new[] {"--arg", "four"};
            Flagrant.Parse(arguments)
                .Enum("arg", TestEnum.Three, out var result);
            Assert.Equal(TestEnum.Three, result);
        }
        
        // Custom
        [Fact]
        public void Custom_returns_Flagrant_instace()
        {
            var flagrant = Flagrant.Parse(new string[0]);
            Assert.IsType<Flagrant>(flagrant.Custom("arg", "hello", a => a, out var result));
        }

        [Fact]
        public void Custom_parses_given_argument()
        {
            var arguments = new[] {"--arg", "golden retriever"};
            string Func(string s) => s.Equals("golden retriever") ? "dog" : "not dog";
            Flagrant.Parse(arguments)
                .Custom("arg", "not dog", Func, out var result);
            Assert.Equal("dog", result); 
        }
        
        // Misc
        [Fact]
        public void Flagrant_flags_can_be_chained()
        {
            var arguments = new[]
            {
                "--string", "hello",
                "--int", "42",
                "--long", "100",
                "--float", "12.12",
                "--double", "77.77",
                "--bool",
                "--enum", "three",
            };
            Flagrant.Parse(arguments)
                .String("string", "goodbye", out var s)
                .Int("int", 100, out var i)
                .Long("long", 1L, out var l)
                .Float("float", 1.5f, out var f)
                .Double("double", 2.5f, out var d)
                .Bool("bool", false, out var b)
                .Enum("enum", TestEnum.One, out var e);
            Assert.Equal("hello", s);
            Assert.Equal(42, i);
            Assert.Equal(100L, l);
            Assert.Equal(12.12f, f);
            Assert.Equal(77.77, d);
            Assert.True(b);
            Assert.Equal(TestEnum.Three, e);
        }
    }
}