using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SecureAutomatedSystem.ProcessEmulation {
   public static class DBConnection {
        private static databaseDataSet DBDataSet = new databaseDataSet();
        private static databaseDataSetTableAdapters.productTableAdapter ProductTableAdapter = new databaseDataSetTableAdapters.productTableAdapter();

        public static void PullNewProduct(Product NewProduct) {            
            databaseDataSet.productRow NewProductRow = DBDataSet.product.NewproductRow();
            NewProductRow.outer_diameter = NewProduct.OuterDiameter.ToString();
            NewProductRow.outer_radius = NewProduct.OuterRadius.ToString();
            NewProductRow.inner_diameter = NewProduct.InnerDiameter.ToString();
            NewProductRow.inner_radius = NewProduct.InnerRadius.ToString();
            NewProductRow.outer_pairing_radius = NewProduct.OuterPairingRadius.ToString();
            NewProductRow.wall_thickness = NewProduct.WallThickness.ToString();
            NewProductRow.top_thickness = NewProduct.TopThickness.ToString();
            NewProductRow.bottom_lowersection_height = NewProduct.BottomLowersectionHeight.ToString();
            NewProductRow.top_diameter = NewProduct.TopDiameter.ToString();
            NewProductRow.outer_pairing_radius_cylindrical_ogive = NewProduct.OuterPairingRadiusCyl.ToString();
            DBDataSet.product.AddproductRow(NewProductRow);
            ProductTableAdapter.Update(DBDataSet.product);
        }

    }
}