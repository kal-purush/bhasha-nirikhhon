using System;
using System.Windows;
using DotNetBay.Core;
using DotNetBay.Model;
using DotNetBay.WPF.ViewModel;

namespace DotNetBay.WPF.View
{
    /// <summary>
    /// Interaction logic for BidView.xaml
    /// </summary>
    public partial class BidView
    {
        public BidView(Auction selectedAuction) {
            InitializeComponent();

            var app = Application.Current as App;

            var simpleMemberService = new SimpleMemberService(app.MainRepository);
            var auctionService = new AuctionService(app.MainRepository, simpleMemberService);

            DataContext = new BidViewModel(selectedAuction, auctionService, simpleMemberService);
        }
    }
}