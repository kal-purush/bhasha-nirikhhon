using SecureAutomatedSystem.ProcessEmulation;

namespace SecureAutomatedSystem.Utils {
    public static class DBUtils {
        private static databaseDataSet dataSet = new databaseDataSet();

        private static databaseDataSetTableAdapters.productTableAdapter productTableAdapter = new databaseDataSetTableAdapters.productTableAdapter();

        delegate string AssignDelgate(float value);

        public static void PullNewProduct(Product newProduct) {
            dataSet.product.AddproductRow(CreateDataRow(newProduct, x => AssignParameter(x)));
            productTableAdapter.Update(dataSet.product);
        }

        public static void PullNewProductWithEnctyprion(Product newProduct, string key) {
            dataSet.product.AddproductRow(CreateDataRow(newProduct, x => AssignParameter(x, key)));
            productTableAdapter.Update(dataSet.product);
        }

        private static databaseDataSet.productRow CreateDataRow(Product newProduct, AssignDelgate assign) {
            databaseDataSet.productRow newProductRow = dataSet.product.NewproductRow();
            newProductRow.outer_diameter = assign(newProduct.OuterDiameter);
            newProductRow.outer_radius = assign(newProduct.OuterRadius);
            newProductRow.inner_diameter = assign(newProduct.InnerDiameter);
            newProductRow.inner_radius = assign(newProduct.InnerRadius);
            newProductRow.outer_pairing_radius = assign(newProduct.OuterPairingRadius);
            newProductRow.wall_thickness = assign(newProduct.WallThickness);
            newProductRow.top_thickness = assign(newProduct.TopThickness);
            newProductRow.bottom_lowersection_height = assign(newProduct.BottomLowersectionHeight);
            newProductRow.top_diameter = assign(newProduct.TopDiameter);
            newProductRow.outer_pairing_radius_cylindrical_ogive = assign(newProduct.OuterPairingRadiusCyl);
            return newProductRow;
        }

        static string AssignParameter(float value) {
            return value.ToString();
        }

        static string AssignParameter(float value, string key) {
            return CryptoUtils.EncryptParameter(value, key);
        }
    }
}