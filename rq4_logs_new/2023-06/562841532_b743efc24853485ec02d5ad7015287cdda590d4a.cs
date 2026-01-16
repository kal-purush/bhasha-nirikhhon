using AutoMapper;
using Booking_Movie.Data.Entities;
using Booking_Movie.Data.Infrastructure;
using Booking_Movie.Data.Repositories;
using Booking_Movie.ViewModel.Catalog.ShowTimeVM;
using Booking_Movie.ViewModel.Common;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Booking_Movie.Application.Catalog.ShowTimes
{
    public class ShowTimeService : IShowTimeService
    {

        private readonly ShowTimeRepository _showTimeRepository;
        private readonly IUnitOfWork _unitOfWork;
        private readonly IMapper _mapper;
        public ShowTimeService(ShowTimeRepository showTimeRepository, IUnitOfWork unitOfWork, IMapper mapper)
        {
            _showTimeRepository = showTimeRepository;
            _unitOfWork = unitOfWork;
            _mapper = mapper;

        }
        public async Task<PagedResult<ShowTimeViewModel>> GetAllPaging(GetShowTimePagingRequest pagingRequest)
        {
            //1. query
            int total = 0;
            IQueryable<ShowTime?> query = null!;
            Expression<Func<ShowTime, bool>> expression = null!;

            //2. filter
            //if (!String.IsNullOrEmpty(pagingRequest.Nationality))
            //{
            //    //query = query.Where();
            //    expression = q => q.Nationality.Name.ToUpper().Contains(pagingRequest.Nationality.ToUpper());
            //}

            query = _showTimeRepository.GetMultiPaging(expression, out total, pagingRequest.PageIndex.Value, pagingRequest.PageSize);


            //3.paging

            //var totalRow = await query!.CountAsync();

            //query = query!.Skip((pagingRequest.PageIndex - 1) * pagingRequest.PageSize)
            //    .Take(pagingRequest.PageSize);

            //if (pagingRequest.SortBy == ViewModel.Enums.SortByActor.ViewCount)
            //{
            //    query = query.OrderBy(q => q!.ViewCount);
            //}

            //4. Get data

            var data = await query.ToListAsync();

            var dataMapper = _mapper.Map<List<ShowTimeViewModel>>(data);

            var pagedResult = new PagedResult<ShowTimeViewModel>()
            {
                Result = dataMapper,
                Total = total,
            };

            return pagedResult;
        }
    }
}