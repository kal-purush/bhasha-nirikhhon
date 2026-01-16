using WebScrap.Common.Tools;
using WebScrap.Common.Tags;
using WebScrap.Common.Tags.Creators;

namespace WebScrap.Tags.Creators;

public class TagCreatorResolver : ITagCreatorResolver
{
    public ITagCreator Resolve(ReadOnlySpan<char> tag)
    {
        tag = tag.Clip("<", ">");
        return tag.IndexOf('/') switch
        {
            0 => new ClosingTagCreator(),
            var i 
                when i == (tag.Length - 1)
              => new InlineTagCreator(),
            _ => new OpeningTagCreator(),
        };
    }
}