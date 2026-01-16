using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using CoolBytes.Core.Extensions;

namespace CoolBytes.Core.Models
{
    public class BlogPostTagCollection : Collection<BlogPostTag>
    {
        protected override void InsertItem(int index, BlogPostTag item)
        {
            item.IsNotNull();

            if (!Exists(item))
                base.InsertItem(index, item);
        }

        public void AddRange(IEnumerable<BlogPostTag> blogPostTags)
        {
            foreach (var blogPostTag in blogPostTags)
            {
                Add(blogPostTag);
            }
        }

        public void RemoveRange(IEnumerable<BlogPostTag> blogPostTags)
        {
            foreach (var blogPostTag in blogPostTags)
            {
                Remove(blogPostTag);
            }
        }

        public void Update(IEnumerable<BlogPostTag> blogPostTags)
        {
            var itemsRemoved = Items.Except(blogPostTags, BlogPostTag.NameComparer).ToArray();

            RemoveRange(itemsRemoved);
            AddRange(blogPostTags);
        }

        private bool Exists(BlogPostTag item) => 
            this.Items.Any(b => b.Name.Equals(item.Name, StringComparison.OrdinalIgnoreCase));
    }
}