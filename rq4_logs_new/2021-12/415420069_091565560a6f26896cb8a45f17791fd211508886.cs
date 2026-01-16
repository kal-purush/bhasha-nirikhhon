using BusinessLogic.Converter;
using BusinessLogic.Interfaces;
using BusinessLogic.Models;
using DAL.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace BusinessLogic.functions
{
    public class Page : ILogicPage
    {
        private IPage _Page;

        PageConverter _PageConverter;
        /// <summary>
        /// Depends on and expects given interfaces(IPage) which realizes within the given DAL. So essentially, this class (Page) needs IPage and PageConverter to work.
        /// </summary>
        /// <param name="PageDAL"></param>
        public Page(IPage PageDAL, PageConverter pageConverter)
        {
            _Page = PageDAL;
            _PageConverter = pageConverter;
        }

        public void Delete(int ID)
        {
            _Page.DeletePage(ID);
        }

        public void Update(PageModel pageModel)
        {
            _Page.EditPage(_PageConverter.Convert_To_DTO_PageModel(pageModel));
        }
    }
}