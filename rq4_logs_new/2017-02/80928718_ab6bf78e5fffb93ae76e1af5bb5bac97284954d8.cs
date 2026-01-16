using System;
using MvvmCross.Platform.Converters;
using XabluApp.Core;

namespace XabluApp.Droid
{
    public class DrawerIconValueConverter : MvxValueConverter<DrawerCellViewModel.DrawerCellType, int>
    {
        protected override int Convert(DrawerCellViewModel.DrawerCellType value, Type targetType, object parameter, System.Globalization.CultureInfo culture)
        {
            switch (value)
            {
                case DrawerCellViewModel.DrawerCellType.About:
                    return Resource.Mipmap.ic_public_black_24dp;
                case DrawerCellViewModel.DrawerCellType.Blogs:
                    return Resource.Mipmap.ic_assignment_black_24dp;
                case DrawerCellViewModel.DrawerCellType.Employees:
                    return Resource.Mipmap.ic_child_care_black_24dp;
                default:
                    return Resource.Mipmap.ic_public_black_24dp;
            }
        }
    }
}