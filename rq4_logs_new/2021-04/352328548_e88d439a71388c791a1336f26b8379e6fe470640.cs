using AutoMapper;
using Data_;
using Data_.Dtos;
using Data_.Interfaces;
using Data_.Validators;
using Microsoft.EntityFrameworkCore;
using Services.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Services
{
    public class AchievementService : IAchievementService
    {
        private IUnitOfWork UnitOfWork;
        private readonly IMapper Mapper;

        public AchievementService(GmfctnContext Context, IMapper _Mapper)
        {
            UnitOfWork = new UnitOfWork(Context); ;
            Mapper = _Mapper;
        }
        public async Task<bool> DeleteAchievement(Guid Id, CancellationToken Cancel)
        {
            try
            {
                if ((await UnitOfWork.AchievementRepository
                .DbSet.FirstOrDefaultAsync(item => item.Id == Id)) != null)
                {
                    await UnitOfWork.AchievementRepository.Delete(Id, Cancel);
                    await UnitOfWork.SaveChangesAsync(Cancel);
                    return true;
                }
                else 
                {
                    return false;
                }
            }
            catch (Exception exc)
            {

                throw exc;
            }
        }

        public async Task<Achievement> GetAchievementById(Guid Id, CancellationToken Cancel)
        {
            return await UnitOfWork.AchievementRepository.GetById(Id, Cancel);
        }

        public async Task<IEnumerable<Achievement>> GetAllAchievements(CancellationToken Cancel)
        {
            return await UnitOfWork.AchievementRepository.GetAll(Cancel);   
        }

        public async Task<bool> CreateAchievement(AchievementCreateDTO Achievement, CancellationToken Cancel)
        {
            try
            {
                if (ModelsValidator.AchievementIsValid((Mapper.Map<AchievementUpdateDTO>(Achievement))))
                {
                    var _Achievement = Mapper.Map<Achievement>(Achievement);
                    _Achievement.Id = new Guid();
                    await UnitOfWork.AchievementRepository.Create(_Achievement, Cancel);
                    await UnitOfWork.SaveChangesAsync(Cancel);
                    return true;
                }
                else
                {
                    return false;
                }
                
            }
            catch (Exception exc)
            {
                throw exc;
            }
            
        }

        public async Task<bool> UpdateAchievement(Guid Id, AchievementUpdateDTO Achievement, CancellationToken Cancel)
        {
            try
            {
                if (ModelsValidator.AchievementIsValid(Achievement) && (await UnitOfWork.AchievementRepository
                    .DbSet.FirstOrDefaultAsync(item => item.Id == Id)) != null)
                {
                    var _Achievement = await UnitOfWork.AchievementRepository.DbSet.FirstOrDefaultAsync(item => item.Id == Id);
                    Mapper.Map(Achievement, _Achievement);
                    UnitOfWork.AchievementRepository.Update(_Achievement);
                    await UnitOfWork.SaveChangesAsync(Cancel);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception exc)
            {
                throw exc;
            }
        }
    }
}