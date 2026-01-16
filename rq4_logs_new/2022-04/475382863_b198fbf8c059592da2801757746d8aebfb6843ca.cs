using System;
using MVP_Proposal2.Scripts.Domain.Repositories;
using MVP_Proposal2.Scripts.Infrastructure;

namespace MVP_Proposal2.Scripts.Providers
{
    public static class DependencyProvider 
    {
        private static readonly Lazy<IShopBundlesRepository> _shopBundlesRepository = 
            new Lazy<IShopBundlesRepository>(() =>new ShopBundlesRepository(new ShopApiGateway()));
        
        public static IShopBundlesRepository ShopBundlesRepository => _shopBundlesRepository.Value;
        
        
        private static readonly Lazy<IPlayerInventoryRepository> _playerInventoryRepository = 
            new Lazy<IPlayerInventoryRepository>(() => new PlayerInventoryRepository());
        
        public static IPlayerInventoryRepository PlayerInventoryRepository => _playerInventoryRepository.Value;
    }
}