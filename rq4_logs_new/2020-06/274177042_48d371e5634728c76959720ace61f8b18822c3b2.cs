using DPLRef.eCommerce.Accessors.DataTransferObjects;
using DPLRef.eCommerce.Common.Shared;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DPLRef.eCommerce.Accessors.Catalog
{
    class SearchAccessor : AccessorBase, ISearchAccessor
    {
        public void RebuildIndex(int catalogId)
        {
            if (System.IO.Directory.Exists(GetIndexDirectory().FullName))
                System.IO.Directory.Delete(GetIndexDirectory().FullName, true);

            using (var analyzer = new SimpleAnalyzer(LuceneVersion.LUCENE_48))
            using (var indexDir = FSDirectory.Open(GetIndexDirectory()))
            {
                var config = new IndexWriterConfig(Lucene.Net.Util.LuceneVersion.LUCENE_48, analyzer);

                using (var indexWriter = new IndexWriter(indexDir, config))
                {
                    using (var db = eCommerce.Accessors.EntityFramework.eCommerceDbContext.Create())
                    {
                        foreach (var p in db.Products.Where(p => p.CatalogId == catalogId))
                        {
                            var doc = new Document();
                            doc.Add(new Int32Field("Id", p.Id, Field.Store.YES));
                            doc.Add(new TextField("Name", p.Name, Field.Store.YES));
                            indexWriter.AddDocument(doc);
                        }
                    }
                }
            }
        }

        public Product[] Search(int catalogId, string text)
        {
            var result = new List<Product>();

            using (var analyzer = new SimpleAnalyzer(LuceneVersion.LUCENE_48))
            using (var indexDirectory = FSDirectory.Open(GetIndexDirectory()))
            using (var indexReader = DirectoryReader.Open(indexDirectory))
            {
                var query = new FuzzyQuery(new Term("Name", text), 2);

                var indexSearcher = new IndexSearcher(indexReader);

                var searchResults = indexSearcher.Search(query, 10).ScoreDocs;

                foreach (var searchResultItem in searchResults)
                {
                    var doc = indexSearcher.Doc(searchResultItem.Doc);

                    var product = new Product()
                    {
                        Id = (int)doc.GetField("Id")?.GetInt32Value(),
                        Name = doc.GetField("Name")?.GetStringValue()
                    };
                    result.Add(product);
                }

                return result.ToArray();
            }
        }

        private DirectoryInfo GetIndexDirectory()
        {
            if (!string.IsNullOrWhiteSpace(Config.IndexPath))
            {
                var dir = new DirectoryInfo(Config.IndexPath);
                if (dir.Exists)
                    return dir;
            }
            throw new InvalidOperationException("eCommerceIndexPath is not configured.");
        }

    }
}