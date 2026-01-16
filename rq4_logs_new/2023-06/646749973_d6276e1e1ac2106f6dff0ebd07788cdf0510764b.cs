using Storage.Domain;
using Storage.Domain.BusnessLayer;
using Storage.Domain.Repositories;

namespace Storage.Application.BusinessLayer
{
    public class ProductService : IProductService
    {
        private readonly IProductRepository _productRepository;

        public ProductService(IProductRepository productRepository)
        {
            _productRepository = productRepository;
        }

        public List<Product> GetProductList()
        {
            var result = _productRepository.GetList();
            return result;
        }
        public Product GetProductById(int id)
        {
            var result = _productRepository.GetById(id);
            return result;
        }
        public void DeleteProductById(int id)
        {
            _productRepository.DeleteById(id);
        }
        public void UpdateProduct(Product p)
        {
            _productRepository.Update(p);
        }
        public Product InsertProduct(Product p)
        {
            var result = _productRepository.Insert(p);
            return result;
        }
        public List<Product> SearchProducts(ProductFilter filter)
        {
            var products = _productRepository.GetList(filter);
            return products;
        }
    }
}