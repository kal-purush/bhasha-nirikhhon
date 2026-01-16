using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace XabluApp.Core
{
    public class BlogClient : IBlogClient
    {
        public BlogClient()
        {
            SetBlogData();
        }

        public async Task<BlogModel> GetBlog(int id)
        {
            return await Task.FromResult(Blogs.FirstOrDefault(b => b.Id == id));
        }

        public async Task<IEnumerable<BlogModel>> GetBlogs()
        {
            return await Task.FromResult(Blogs);
        }

        private List<BlogModel> Blogs = new List<BlogModel>();
        private void SetBlogData()
        {
            int id = 0;
            Blogs = new List<BlogModel>()
            {
                new BlogModel(){ Id = id++, Title = "Xamarin Partner Summit Amsterdam", Author = "CHRIS LAMONT", ImageName = "partnersummit", Url = "https://www.xablu.com/2017/01/31/xamarin-partner-summit-amsterdam/"},
                new BlogModel(){ Id = id++, Title = "Connect(); // 2016 – Developer Tools recap", Author = "HARRY TEN BERGE", ImageName = "developertools", Url = "https://www.xablu.com/2016/11/17/msftconnect/"},
                new BlogModel(){ Id = id++, Title = ".NET standard", Author = "MARTIJN VAN DIJK", ImageName = "netstandard", Url = "https://www.xablu.com/2016/10/20/net-standard/"},
                new BlogModel(){ Id = id++, Title = "TechDays 2016 wrap up", Author = "HARRY TEN BERGE", ImageName = "techdays", Url = "https://www.xablu.com/2016/10/12/techdays-2016-wrap/"},
                new BlogModel(){ Id = id++, Title = "XABLU becomes Xamarin Premier Consulting Partner", Author = "HARRY TEN BERGE", ImageName = "xablupremierpartner", Url = "https://www.xablu.com/2016/10/06/xamarin-premier-partner/"},
                new BlogModel(){ Id = id++, Title = "UI/UX Review", Author = "TIRZA VAN DIJK", ImageName = "uiuxreview", Url = "https://www.xablu.com/2016/07/15/uiux-review/"},
                new BlogModel(){ Id = id++, Title = "Microsoft releases .NET Core 1.0", Author = "HARRY TEN BERGE", ImageName = "corerelease", Url = "https://www.xablu.com/2016/07/01/microsoft-releases-net-core-1-0/"},
                new BlogModel(){ Id = id++, Title = "Should we port our app to Xamarin?", Author = "HARRY TEN BERGE", ImageName = "porttoxamarin", Url = "https://www.xablu.com/2016/06/09/should-we-port-our-app-to-xamarin/"}
            };
        }
    }
}