using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using CrawlerCommon.TagDef.StrictXHTML;

namespace CrawlerCommon
{
    static public class HtmlBuilder
    {
        static public IEnumerable<Token> BuildTree(this IEnumerable<string> list, DefaultBuilder builder) 
        {
            //DefaultBuilder builder = new DefaultBuilder();
            foreach (var item in list)
            {
                Token found = builder.Construct(item);
                if (found!=null)
                    yield return found;
            }
        }
    }

    public class DefaultBuilder 
    {
        public DefaultBuilder()
            : this(
                    typeof(Td), typeof(Hyperlink), typeof(Tr), 
                    typeof(Br),
                    typeof(Img),
                    typeof(CommentTag),
                    typeof(ScriptTag),
                    typeof(ListItem), 
                    typeof(Strong), typeof(BaseH), typeof(Paragraph), typeof(Span), typeof(Div), typeof(Center),
                    typeof(Bold), typeof(Italic), typeof(TeleType), typeof(SubScript), typeof(SuperScript), typeof(Font), typeof(Big), typeof(Small), typeof(Hr), 
                    typeof(TableTag), typeof(Th), typeof(TBody), typeof(THead), typeof(TFoot), typeof(Col), typeof(ColGroup), typeof(Caption),
                    typeof(Input), typeof(SelectTag), typeof(OptionTag), typeof(OptGroup), typeof(TextArea), typeof(ButtonTag), typeof(LabelTag), typeof(Fieldset), typeof(Legend), 
                    typeof(FormTag),
                    typeof(OrderedList), typeof(UnorderedList),
                    typeof(BlockQuote), typeof(Pre),
                    typeof(DefinitionList),
                    typeof(Map), typeof(Area),
                    typeof(Html), typeof(Head), typeof(Title), typeof(Body), typeof(Style),
                    typeof(Declaration), 
                    typeof(Meta), typeof(Link),
                    typeof(Param), typeof(ObjectTag),
                    typeof(UnknownTag), 
                    typeof(Literal))
        { }
        public DefaultBuilder(params Type[] tagTypes)
        {
            //for this type of chain, the root is last identifier, and the root is at end of this chain
            foreach (var item in tagTypes.Reverse<Type>())
                if (typeof(Token).IsAssignableFrom(item))
                    this.IdentifierChain = (Token)Activator.CreateInstance(item, this.IdentifierChain, "<a>");
        }

        protected Token IdentifierChain = null;
        public Document ParsedTree { get; protected set; }
        protected Node _stacktop; //virtual stack treating node as stack, pops are traversed toward parent
        virtual public Token Construct(string symbol)
        {
            if (this.ParsedTree == null)
            {
                this.ParsedTree = new Document();
                _stacktop = this.ParsedTree;
            }
            
            Token found = null;
            Node stacktop = (Node)this._stacktop;
            Token identChain = this.IdentifierChain;
            while (found == null && identChain!=null)
            {
                found = identChain.LikeIdentify(symbol, ref stacktop);
                identChain = identChain.ParentNode;
            }
            if (found == null)
                throw new InvalidOperationException("did not expect " + symbol + ", in " + stacktop.ToString());

            this._stacktop = stacktop;
            return found;
        }

        virtual public List<Tag> SupportedTags()
        {
            List<Tag> tags = new List<Tag>();
            var tagidentifier = this.IdentifierChain;
            while (tagidentifier != null)
            {
                if (tagidentifier is Tag)
                    tags.Add((Tag)tagidentifier);
                tagidentifier = tagidentifier.ParentNode;
            }
            return tags;
        }
    }

    public class TrackingBuilder : DefaultBuilder
    {
        public TrackingBuilder() : base() { }
        public TrackingBuilder(params Type[] tagTypes) : base(tagTypes) { }


        Token[] scanorder = null;
        override public Token Construct(string symbol)
        {
            if (this.ParsedTree == null)
            {
                this.ParsedTree = new Document();
                this._stacktop = this.ParsedTree;
            }
            if (scanorder == null)
            {
                List<Token> temp = new List<Token>();
                while (this.IdentifierChain != null)
                {
                    temp.Add(this.IdentifierChain);
                    this.IdentifierChain = this.IdentifierChain.ParentNode;
                }
                scanorder = temp.ToArray();
            }

            Token found = null;
            Node stacktop = (Node)this._stacktop;
            //Token identChain = this.IdentifierChain;

            int i = -1;
            for (i = 0; i < scanorder.Length; i++)
            {
                found = scanorder[i].LikeIdentify(symbol, ref stacktop);
                if (found != null) break;
            }

            if (found == null)
                throw new InvalidOperationException("did not expect " + symbol + ", in " + stacktop.ToString());
            else if (i > 0 && !(scanorder[i] is Closure || scanorder[i] is Literal || scanorder[i] is UnknownTag))
            {
                Token temp = scanorder[i - 1];
                scanorder[i - 1] = scanorder[i];
                scanorder[i] = temp;
            }

            this._stacktop = stacktop;
            return found;
        }

    }

    public class BuilderStats : DefaultBuilder
    {
        public BuilderStats(DefaultBuilder tracked) : base() { this.track = tracked; }

        DefaultBuilder track = null;
        public bool StatsOn = false;
        public readonly Dictionary<string, int> TagStats = new Dictionary<string, int>();
        public long Count { get; private set; }
        public long RunTime { get; private set; }
        public readonly Dictionary<long, string> Unknown = new Dictionary<long, string>();

        override public Token Construct(string symbol)
        {
            int runtime = 0;
            if (StatsOn)
                runtime = Environment.TickCount;

            Token found = this.track.Construct(symbol);
            
            if (StatsOn)
            {
                this.Count++;
                
                string name = found.GetType().FullName;
                if (!TagStats.ContainsKey(name))
                    TagStats.Add(name, 0);
                TagStats[name]++;

                int end = Environment.TickCount;
                if (end > runtime) this.RunTime += (end - runtime);

                if (found is UnknownTag)
                    this.Unknown.Add(this.Count, found.ActualSymbol);
            }

            return found;
        }

    }

    public class IndexedBuilder : DefaultBuilder
    {
        public IndexedBuilder() : base() 
        {
            indexIdentifier();
        }
        public IndexedBuilder(params Type[] tagTypes) : base(tagTypes) 
        {
            indexIdentifier(); 
        }

        void indexIdentifier()
        {
            List<Token> unindexed = new List<Token>();
            while (this.IdentifierChain != null)
            {
                if (this.IdentifierChain is IIndexableParseElement)
                {
                    Token temp = this.IdentifierChain;
                    this.IdentifierChain = this.IdentifierChain.ParentNode;
                    foreach (var index in ((IIndexableParseElement)temp).Traversal)
                        if (!map.Contains(index.ToUpper()))
                        {
                            map.Add(index.ToUpper(), temp);
                            temp.AssignParent(null);
                        }
                        else
                        {
                            temp.AssignParent(map[index.ToUpper()]);
                            map[index.ToUpper()] = temp;
                        }
                }
                else
                {
                    unindexed.Add(this.IdentifierChain);
                    this.IdentifierChain = this.IdentifierChain.ParentNode;
                }
            }

            foreach (var item in unindexed)
            {
                if (this.IdentifierChain == null)
                {
                    item.AssignParent(null);
                    this.IdentifierChain = item;
                }
                else
                {
                    item.AssignParent(this.IdentifierChain);
                    this.IdentifierChain.AssignParent(item);
                }
                //this.IdentifierChain = item;
            }
        }

        PathIndex<Token> map = new PathIndex<Token>();
        override public Token Construct(string symbol) {
            Token found = ConstructFromIndex(symbol);
            if (found != null)
                return found;
            return base.Construct(symbol);
        }

        virtual public Token ConstructFromIndex(string symbol)
        {
            if (this.ParsedTree == null)
            {
                this.ParsedTree = new Document();
                _stacktop = this.ParsedTree;
            }

            Token found = null;
            Node stacktop = (Node)this._stacktop;
            string search = symbol.ToUpper();
            Token identChain = map.GetOffPathValue(search);
            if(identChain==null)
                return null;

            while (found == null && identChain!=null)
            {
                found = identChain.LikeIdentify(symbol, ref stacktop);
                identChain = identChain.ParentNode;
            }
            if (found == null)
                return null;

            this._stacktop = stacktop;
            return found;
        }
    }

    public class PathIndex<T>
    {
        public T DefaultValue;
        IndexNode<T> root = null;
        public void Add(string key, T value)
        {
            this.Add(convert(key), value);
        }
        public void Add(IEnumerable<byte> key, T value)
        {
            IndexNode<T> start = root;
            foreach (byte keypart in key)
            {
                if (root == null)
                {
                    root = new IndexNode<T>();
                    start = root;
                }

                if (start.Table.ContainsKey(keypart))
                {
                    start = start.Table[keypart];
                }
                else
                {
                    start.Table.Add(keypart, new IndexNode<T>());
                    start = start.Table[keypart];
                }
            }

            if (start.Value != null)
                throw new ArgumentException("Item already added with that key");

            start.Value = value;
        }

        public T this[string key]
        {
            get
            {
                return this[convert(key)];
            }
            set
            {
                this[convert(key)] = value;
            }
        }
        public T this[IEnumerable<byte> key]
        {
            get
            {
                IndexNode<T> best = bestGet(key);
                if (best == null)
                    return this.DefaultValue;
                else
                    return best.Value;
            }
            set
            {
                IndexNode<T> exact = exactGet(key);
                if (exact == null)
                    throw new KeyNotFoundException();
                else if (exact.Value==null)
                    throw new KeyNotFoundException();
                else
                    exact.Value = value;
            }
        }

        public T GetOffPathValue(string key)
        {
            return GetOffPathValue(convert(key));
        }
        public T GetOffPathValue(IEnumerable<byte> key)
        {
            IndexNode<T> best = bestGet(key);
            if (best == null)
                return this.DefaultValue;
            else
                return best.Value;
        }

        public bool ContainsOffPathValue(string key)
        {
            return this.ContainsOffPathValue(convert(key));
        }
        public bool ContainsOffPathValue(IEnumerable<byte> key)
        {
            IndexNode<T> best = bestGet(key);
            if (best == null)
                return false;
            else
                return best.Value!=null;
        }
        public bool Contains(string key)
        {
            return this.Contains(convert(key));
        }
        public bool Contains(IEnumerable<byte> key)
        {
            IndexNode<T> exact = exactGet(key);
            if (exact == null)
                return false;
            else
                return exact.Value != null;
        }

        IndexNode<T> bestGet(IEnumerable<byte> key)
        {
            if (root == null)
                return null;

            IndexNode<T> start = root;
            foreach (byte keypart in key)
            {
                IndexNode<T> prev = start;
                if (start.Table.ContainsKey(keypart))
                    start = start.Table[keypart];
                else
                    return start;
            }
            return start;
        }
        IndexNode<T> exactGet(IEnumerable<byte> key)
        {
            if (root == null)
                return null;

            IndexNode<T> start = root;
            foreach (byte keypart in key)
            {
                IndexNode<T> prev = start;
                if (start.Table.ContainsKey(keypart))
                    start = start.Table[keypart];
                else
                    return null;
            }
            return start;
        }

        IEnumerable<byte> convert(string value) 
        {
            foreach (char letter in value)
                foreach (var item in BitConverter.GetBytes(letter)) 
                    yield return item; 
        }

        private class IndexNode<Q>
        {
            public readonly SortedList<byte, IndexNode<Q>> Table = new SortedList<byte, IndexNode<Q>>();
            public Q Value;
        }
    }
}