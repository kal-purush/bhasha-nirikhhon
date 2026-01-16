using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EcommerceApp.Entities;
using EcommerceApp.Util;
using Microsoft.Data.SqlClient;
using EcommerceApp.Exceptions;


namespace EcommerceApp.DAO
{
    public class ProductInfoDao : IProductInfoDao<ProductInfo>
    {
        SqlConnection sqlCon = DBConnectionUtil.GetConnection("AppSettings.json");
        SqlCommand cmd = new SqlCommand();
        SqlDataReader dr;
        public bool DeleteProductInfo(int productId)
        {
            try
            {
                cmd.Connection = sqlCon;
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = queryBuilder.Append($"delete from ProductInfo where ProductId={productId}");
                cmd.CommandText = queryBuilder.ToString();
                if (sqlCon.State == System.Data.ConnectionState.Closed)
                {
                    sqlCon.Open();
                }
                cmd.ExecuteNonQuery();//insert/delete/update
                return true;
            }
            catch (SqlException ex)
            {
                return false;
            }

        }
        public List<ProductInfo> GetAllProductInfo()
        {
            List<ProductInfo> productInfos = new List<ProductInfo>();

            try
            {
                cmd.Connection = sqlCon;
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = queryBuilder.Append($"select * from ProductInfo");
                cmd.CommandText = queryBuilder.ToString();
                if (sqlCon.State == System.Data.ConnectionState.Closed)
                {
                    sqlCon.Open();
                }
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        ProductInfo productInfo = new ProductInfo();
                        productInfo.ProductId = Convert.ToInt32(dr["ProductId"]);
                        productInfo.ProductName = dr["ProductName"].ToString();
                        productInfo.ListPrice = Convert.ToDouble(dr["ProductPrice"]);
                        productInfos.Add(productInfo);
                    }
                    return productInfos;
                }

            }
            catch (SqlException ex)
            {

            }
            finally
            {
                dr?.Close();
                sqlCon.Close();
            }
            return null;
        }
        public ProductInfo GetProductInfoById(int produtId)
        {
            ProductInfo productInfo = new ProductInfo();
            try
            {
                cmd.Connection = sqlCon;
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = queryBuilder.Append($"select * from ProductInfo where ProductId={produtId}");
                cmd.CommandText = queryBuilder.ToString();
                if (sqlCon.State == System.Data.ConnectionState.Closed)
                {
                    sqlCon.Open();
                }
                dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        productInfo.ProductId = Convert.ToInt32(dr["ProductId"]);
                        productInfo.ProductName = dr["ProductName"].ToString();
                        productInfo.ListPrice = Convert.ToDouble(dr["ProductPrice"]);
                    }
                    return productInfo;
                }
            }
            catch (SqlException ex)
            {
                throw new ProductNotFoundException("Product not found");
            }
            finally
            {
                dr?.Close();
                sqlCon.Close();
            }
            return null;
        }
        public ProductInfo SaveProductInfo(ProductInfo productInfo)
        {
            try
            {
                cmd.Connection = sqlCon;
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = queryBuilder.Append($"insert into ProductInfo(ProductName,ProductPrice) values('{productInfo.ProductName}',{productInfo.ListPrice})");
                cmd.CommandText = queryBuilder.ToString();
                if (sqlCon.State == System.Data.ConnectionState.Closed)
                {
                    sqlCon.Open();
                }
                cmd.ExecuteNonQuery();//insert/delete/update
                return productInfo;
            }
            catch (SqlException ex)
            {
                throw new ProductNotFoundException("Product not found");
            }
            finally
            {
                sqlCon.Close();
            }
        }
        public ProductInfo UpdateProductInfo(ProductInfo productInfo)
        {
            try
            {
                cmd.Connection = sqlCon;
                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder = queryBuilder.Append($"update ProductInfo set ProductName='{productInfo.ProductName}',ProductPrice={productInfo.ListPrice} where ProductId={productInfo.ProductId}");
                cmd.CommandText = queryBuilder.ToString();
                if (sqlCon.State == System.Data.ConnectionState.Closed)
                {
                    sqlCon.Open();
                }
                cmd.ExecuteNonQuery();//insert/delete/update
                return productInfo;
            }
            catch (SqlException ex)
            {
                throw new ProductNotFoundException("Product not found");
            }
            finally
            {
                sqlCon.Close();
            }
        }
    }
}