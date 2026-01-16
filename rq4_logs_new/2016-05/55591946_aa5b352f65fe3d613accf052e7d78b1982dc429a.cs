using SecureAutomatedSystem.ProcessEmulation;
using SecureAutomatedSystem.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SecureAutomatedSystem.Recomendation {
    public class Recomendations {
        public static Product GetAccuracy() {
            Product perfectProduct = new Product();
            perfectProduct.DeserializeFromXml("SuggestedParams.xml");
            //string recomendations = null;
            databaseDataSet.productDataTable data = DBUtils.ProductTableAdapter.GetData();
            Product averageProductData = new Product();
            float outerDiameter = 0;
            float topThickness = 0;
            float innerDiameter = 0;
            float bottomLowerSection = 0;
            float outerPairingRadius = 0;
            float topDiameter = 0;
            float wallThickness = 0;
            float outerPairingRadCyl = 0;
            if (data.Count == 0) return null;

            for (int i = 0; i < data.Count; i++) {
                outerDiameter += (Math.Abs(float.Parse(data[i].outer_diameter) - perfectProduct.OuterDiameter)/
                                  perfectProduct.OuterDiameter)*100;
                topThickness += (Math.Abs(float.Parse(data[i].top_thickness) - perfectProduct.TopThickness)/
                                 perfectProduct.TopThickness)*100;
                innerDiameter += (Math.Abs(float.Parse(data[i].inner_diameter) - perfectProduct.InnerDiameter)/
                                  perfectProduct.InnerDiameter)*100;
                bottomLowerSection +=
                    (Math.Abs(float.Parse(data[i].bottom_lowersection_height) - perfectProduct.BottomLowersectionHeight)/
                     perfectProduct.BottomLowersectionHeight)*100;
                outerPairingRadius +=
                    (Math.Abs(float.Parse(data[i].outer_pairing_radius) - perfectProduct.OuterPairingRadius)/
                     perfectProduct.OuterPairingRadius)*100;
                topDiameter += (Math.Abs(float.Parse(data[i].top_diameter) - perfectProduct.TopDiameter)/
                                perfectProduct.TopDiameter)*100;
                wallThickness += (Math.Abs(float.Parse(data[i].wall_thickness) - perfectProduct.WallThickness)/
                                  perfectProduct.WallThickness)*100;
                outerPairingRadCyl +=
                    (Math.Abs(float.Parse(data[i].outer_pairing_radius_cylindrical_ogive) -
                              perfectProduct.OuterPairingRadiusCylindricalOgive)/
                     perfectProduct.OuterPairingRadiusCylindricalOgive)*100;
            }


            outerDiameter /= data.Count;
            topThickness /= data.Count;
            innerDiameter /= data.Count;
            bottomLowerSection /= data.Count;
            outerPairingRadius /= data.Count;
            topDiameter /= data.Count;
            wallThickness /= data.Count;
            outerPairingRadCyl /= data.Count;
            Product productAccuracyInDetails = new Product() {
                OuterDiameter = 100 - outerDiameter,
                OuterRadius = 100 - outerDiameter,
                TopThickness = 100 - topThickness,
                InnerDiameter = 100 - innerDiameter,
                InnerRadius = 100 - innerDiameter,
                BottomLowersectionHeight = 100 - bottomLowerSection,
                OuterPairingRadius = 100 - outerPairingRadius,
                TopDiameter = 100 - topDiameter,
                WallThickness = 100 - wallThickness,
                OuterPairingRadiusCylindricalOgive = 100 - outerPairingRadCyl
            };

            return productAccuracyInDetails;

            /*
            StringBuilder sb = new StringBuilder();
            sb.Append("Total products produced: " + data.Count + "\n");
            sb.Append("OuterDiameter accuracy: " + (100 - outerDiameter) + ";\n");
            sb.Append("TopThickness accuracy: " + (100 - topThickness) + ";\n");
            sb.Append("InnerDiameter accuracy: " + (100 - innerDiameter) + ";\n");
            sb.Append("BottomLowerSection accuracy: " + (100 - bottomLowerSection) + ";\n");
            sb.Append("OuterPairingRadius accuracy: " + (100 - outerPairingRadius) + ";\n");
            sb.Append("TopDiameter accuracy: " + (100 - topDiameter) + ";\n");
            sb.Append("WallThickness accuracy: " + (100 - wallThickness) + ";\n");
            sb.Append("OuterPairingRadCylindrical accuracy: " + (100 - outerPairingRadCyl) + ";\n");

            recomendations = sb.ToString(); */
            //if (Emulator.SaveInDB) { }
            //MessageBox.Show(recomendations, "Report");
            //return recomendations;
        }
    }
}