using Domain;
using Domain.Repository.IRepository;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Commands
{
    public class PostCart : IRequest<ShoppingCart>
    {
        public int Id { get; set; }
        public string UserId { get; set; }
        public double SubTotal { get; set; }

        public double Total { get; set; }

        public List<Producto>? Cart_Products { get; set; }

        public DateTime ShoppingDate { get; set; } = DateTime.Now;
    }
    public class PostCarttHandler : IRequestHandler<PostCart, ShoppingCart>


    {
        private readonly IShoppingCartRepository _repository;
        private readonly IDetailsRepository _detailRepository;

        public PostCarttHandler(IShoppingCartRepository repository, IDetailsRepository detailRepository)
        {
            _repository = repository;
            _detailRepository = detailRepository;
        }

        public async Task<ShoppingCart> Handle(PostCart request, CancellationToken cancellationToken)
        {
            ShoppingCart shoppingCart = new ShoppingCart();
            shoppingCart.UserId = request.UserId;
            shoppingCart.SubTotal = request.SubTotal;
            shoppingCart.Total = request.Total;
            shoppingCart.ShoppingDate = request.ShoppingDate;
            shoppingCart.Cart_Products = request.Cart_Products;
            _repository.Add(shoppingCart);
            foreach (var item in request.Cart_Products)
            {
                Details cart = new Details();
                cart.ProductoId = item.Id;
                cart.CartId = shoppingCart.Id;
                cart.Quantity = item.Quantity;

                _detailRepository.Add(cart);
            }
            return shoppingCart;


        }
    }
}