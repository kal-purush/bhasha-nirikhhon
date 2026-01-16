namespace Uniscript.Unittest
{
    using System;
    using FluentAssertions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Uniscript;

    [TestClass]
    public class UniSymbolTableTests
    {
        // Constants.
        private const string Key1 = "Key1";
        private const string Key2 = "Key2";
        private const string Key3 = "Key3";
        private const string Value1 = "Value1";
        private const string Value2 = "Value2";
        private const string Value3 = "Value3";
        private const string Whitespace = "  ";

        private IUniSymbolTable<string> table;

        [TestInitialize]
        public void Setup()
        {
            this.table = new UniSymbolTable<string>();
        }

        /// <summary>
        /// Checks for KeyNotFoundException when key isn't defined in a single level table.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_NotDefinedSingleLevel_ExceptionThrown()
        {
            this.table[Key1] = Value1;

            Action action = () =>
            {
                var foo = this.table[Key2];
            };

            action.ShouldThrow<UniKeyNotFoundException>("Key2 is not defined");
        }

        /// <summary>
        /// Checks for KeyNotFoundException when key isn't defined in a multilevel table.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_NotDefinedMultilevel_ExceptionThrown()
        {
            this.table[Key1] = Value1;
            this.table.Push();
            this.table.Push();

            Action action = () =>
            {
                var foo = this.table[Key2];
            };

            action.ShouldThrow<UniKeyNotFoundException>("Key2 is not defined");
        }

        /// <summary>
        /// Checks for proper storage of values in a single level.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_DefinedSingleLevel_ValuesMatch()
        {
            this.table[Key1] = Value1;
            this.table[Key2] = Value2;

            this.table[Key1].Should().Be(Value1, "value was stored");
            this.table[Key2].Should().Be(Value2, "value was stored");
        }

        /// <summary>
        /// Checks for proper storage and retrieval of values across multiple levels.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_DefinedMultiLevel_ValuesMatch()
        {
            this.table[Key1] = Value1;

            this.table.Push();

            this.table[Key2] = Value2;

            this.table[Key1].Should().Be(Value1, "value was stored");
            this.table[Key2].Should().Be(Value2, "value was stored");
        }

        /// <summary>
        /// Checks that topOnly parameter works on retrieval.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_TopOnlyTrue_DoNotCheckLower()
        {
            this.table[Key1] = Value1;

            this.table.Push();

            this.table[Key2] = Value2;

            Action action = () =>
            {
                var foo = this.table[Key1, true];
            };

            action.ShouldThrow<UniKeyNotFoundException>("Key1 is defined in outer scope");

            this.table[Key2].Should().Be(Value2, "value was stored");
        }

        /// <summary>
        /// Checks that empty, null, and whitespace keys are rejected.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_GetNull_ExceptionThrown()
        {
            Action action1 = () =>
            {
                var foo = this.table[null];
            };

            action1.ShouldThrow<ArgumentException>("key cannot be null");

            Action action2 = () =>
            {
                var foo = this.table[string.Empty];
            };

            action2.ShouldThrow<ArgumentException>("key cannot be empty");

            Action action3 = () =>
            {
                var foo = this.table[Whitespace];
            };

            action3.ShouldThrow<ArgumentException>("key cannot be whitespace");
        }

        /// <summary>
        /// Checks that empty, null, and whitespace values are rejected.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_SetNull_ExceptionThrown()
        {
            Action action1 = () =>
            {
                this.table[Key1] = null;
            };

            action1.ShouldThrow<ArgumentException>("value cannot be null");

            Action action2 = () =>
            {
                this.table[Key2] = string.Empty;
            };

            action2.ShouldThrow<ArgumentException>("key cannot be empty");

            Action action3 = () =>
            {
                this.table[Key3] = Whitespace;
            };

            action3.ShouldThrow<ArgumentException>("key cannot be whitespace");
        }

        /// <summary>
        /// Checks that duplicate keys are rejected.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_SingleLevelDuplicate_ExceptionThrown()
        {
            this.table[Key1] = Value1;

            Action action = () =>
            {
                this.table[Key1] = Value2;
            };

            action.ShouldThrow<UniKeyExistsException>("key already exists in current scope");
        }

        /// <summary>
        /// Checks that duplicate keys are allowed if in different scopes (redefinition of vars).
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Indexer_MultiLevelDuplicate_ExceptionNotThrown()
        {
            this.table[Key1] = Value1;

            this.table.Push();

            Action action = () =>
            {
                this.table[Key1] = Value2;
            };

            action.ShouldNotThrow("key already exists but in different scope");
        }

        /// <summary>
        /// Checks that Depth property matches the Pushes and pops.
        /// </summary>
        [TestMethod]
        [TestCategory("UnitTest")]
        [TestCategory("UniSymbolTable")]
        public void Depth_Matches()
        {
            this.table.Depth.Should().Be(1, "there is only the default scope");

            this.table.Push();
            this.table.Depth.Should().Be(2, "we pushed new scope");

            this.table.Push();
            this.table.Depth.Should().Be(3, "we pushed new scope");

            this.table.Pop();
            this.table.Pop();
            this.table.Depth.Should().Be(1, "we popped new scopes");
        }
    }
}