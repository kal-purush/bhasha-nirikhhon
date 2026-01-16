using DAL.DTO;
using DAL.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace WikiFSUnitTests.Mockups
{
    public class PageDALMockup : IPage, IPageContainer
    {
        public void CreatePage(PageDTO page)
        {
            throw new NotImplementedException();
        }

        public void DeletePage(int ID)
        {
            throw new NotImplementedException();
        }

        public void EditPage(PageDTO page)
        {
            throw new NotImplementedException();
        }

        public List<PageDTO> GetallText()
        {
            throw new NotImplementedException();
        }

        public PageDTO GetPage(int ID)
        {
            throw new NotImplementedException();
        }
    }
}