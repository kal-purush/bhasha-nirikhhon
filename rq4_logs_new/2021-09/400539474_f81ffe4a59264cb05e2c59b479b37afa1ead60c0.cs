using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RR_BL
{
    public class ResRevBL : IResRevBL
    {
        private readonly IResRevBL _repo;
        public ResRevBL(IResRevBL repo)
        {
            _repo = repo;
        }
        public Reviews AddReviews(Reviews reviews)
        {
            return _repo.AddReviews(reviews);
        }
        public List<Reviews> ViewReviews()
        {
            return _repo.ViewReviews();
        }
        public Users AddUser(Users users)
        {
            return _repo.AddUser(users);
        }
        public List<Users> ViewUsers()
        {
            return _repo.ViewUsers();
        }
        public List<Restaurants> ViewRestaurants()
        {
            return _repo.ViewRestaurants();
        }
    }
}