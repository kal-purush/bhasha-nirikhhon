using Glass.Mapper.Sc.Configuration.Attributes;
using Sitecore.Foundation.GlassMapper.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Sitecore.Feature.Media.Models
{
    public class Carousel : SitecoreItem
    {
        [SitecoreField("Images")]
        public virtual IEnumerable<ImageItem> Images { get; set; }
    }
}