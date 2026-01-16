using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using YiSha.Util;
using YiSha.Util.Extension;
using YiSha.Util.Model;
using YiSha.Entity.HotelManage;
using YiSha.Model.Param.HotelManage;
using YiSha.Service.HotelManage;

namespace YiSha.Business.HotelManage
{
    /// <summary>
    /// 创 建：admin
    /// 日 期：2020-11-06 20:38
    /// 描 述：业务类
    /// </summary>
    public class CategoryBLL
    {
        private CategoryService categoryService = new CategoryService();

        #region 获取数据
        public async Task<TData<List<CategoryEntity>>> GetList(CategoryListParam param)
        {
            TData<List<CategoryEntity>> obj = new TData<List<CategoryEntity>>();
            obj.Data = await categoryService.GetList(param);
            obj.Total = obj.Data.Count;
            obj.Tag = 1;
            return obj;
        }

        public async Task<TData<List<CategoryEntity>>> GetPageList(CategoryListParam param, Pagination pagination)
        {
            TData<List<CategoryEntity>> obj = new TData<List<CategoryEntity>>();
            obj.Data = await categoryService.GetPageList(param, pagination);
            obj.Total = pagination.TotalCount;
            obj.Tag = 1;
            return obj;
        }

        public async Task<TData<CategoryEntity>> GetEntity(long id)
        {
            TData<CategoryEntity> obj = new TData<CategoryEntity>();
            obj.Data = await categoryService.GetEntity(id);
            if (obj.Data != null)
            {
                obj.Tag = 1;
            }
            return obj;
        }
        #endregion

        #region 提交数据
        public async Task<TData<string>> SaveForm(CategoryEntity entity)
        {
            TData<string> obj = new TData<string>();
            await categoryService.SaveForm(entity);
            obj.Data = entity.Id.ParseToString();
            obj.Tag = 1;
            return obj;
        }

        public async Task<TData> DeleteForm(string ids)
        {
            TData obj = new TData();
            await categoryService.DeleteForm(ids);
            obj.Tag = 1;
            return obj;
        }
        #endregion

        #region 私有方法
        #endregion
    }
}