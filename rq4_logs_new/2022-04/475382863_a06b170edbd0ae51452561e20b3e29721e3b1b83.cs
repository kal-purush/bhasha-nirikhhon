using MVP_Proposal2.Scripts.Domain.Repositories;
using MVP_Proposal2.Scripts.Infrastructure;
using MVP_Proposal2.Scripts.Views;
using UnityEngine;
using IShopBundlesRepository = MVP_Proposal2.Scripts.Domain.Repositories.IShopBundlesRepository;
using ShopBundlesRepository = MVP_Proposal2.Scripts.Infrastructure.ShopBundlesRepository;

namespace MVP_Proposal2.Scripts
{
    public class ApplicationContext : MonoBehaviour
    {
        [SerializeField] private ShopPopupView shopPopupView;
        
        //TODO: Move to a provider
        private IShopBundlesRepository _shopBundlesRepository;
        public IShopBundlesRepository ShopBundlesRepository => _shopBundlesRepository ?? new ShopBundlesRepository();
        
        private IPlayerInventoryRepository _playerInventoryRepository;
        public IPlayerInventoryRepository PlayerInventoryRepository => _playerInventoryRepository ?? new PlayerInventoryRepository();
        
        void Start()
        {
            InitViews();
            shopPopupView.Show();
        }

        private void InitViews()
        {
            shopPopupView.Init(ShopBundlesRepository, PlayerInventoryRepository);
        }
    }
}