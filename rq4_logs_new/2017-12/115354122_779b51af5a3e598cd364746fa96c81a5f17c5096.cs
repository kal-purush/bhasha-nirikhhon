using System;
using System.Collections;
using System.Windows.Forms;

namespace Tagger
{
    public class AlphabeticSorter : IComparer
    {
        /// <summary>
        /// Verleiche Namen der TreeView-Knoten und sortiere alphabetisch
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        public int Compare(object x, object y)
        {
            if (x is Object && y is Object)
            {
                TreeNode tnX = (TreeNode)x;
                TreeNode tnY = (TreeNode)y;

                // tnX: Zerlege ersten Knoten-Namen in Character und speichere deren Integerwert in Array
                int charCount = 0;
                int[] values_tnX = new int[tnX.Text.Length];

                foreach (char c in tnX.Text)
                {
                    values_tnX[charCount] = Convert.ToInt32(c);
                    charCount++;
                }

                // tnY: Zerlege zweiten Knoten-Namen in Character und speichere deren Integerwert in Array
                charCount = 0;
                int[] values_tnY = new int[tnY.Text.Length];

                foreach (char c in tnY.Text)
                {
                    values_tnY[charCount] = Convert.ToInt32(c);
                    charCount++;
                }

                // Festlegen wieviele Integer mindestens verglichen werden müssen
                int charToBeChecked = 0;

                if (tnX.Text.Length < tnY.Text.Length)
                    charToBeChecked = tnX.Text.Length;
                else if (tnX.Text.Length > tnY.Text.Length)
                    charToBeChecked = tnY.Text.Length;
                else
                    charToBeChecked = tnX.Text.Length;

                // Arrays vergleichen bis zu prüfende Zeichenlänge erreicht, wenn nicht bereits zuvor Rückgabewert.
                // Falls zu prüfende Zeichenlänge erreicht Entscheidung über Namenslänge sonst identisch
                for (int i = 0; i < charToBeChecked; i++)
                {
                    if (values_tnX[i] < values_tnY[i])
                        return -1;
                    else if (values_tnX[i] > values_tnY[i])
                        return 1;
                    else if (i == charToBeChecked - 1)
                    {
                        if (tnX.Text.Length > charToBeChecked)
                            return 1;
                        else if (tnY.Text.Length > charToBeChecked)
                            return -1;
                        else
                            return 0;
                    }
                }
            }

            throw new ArgumentException("Fehler beim alphabetischen Sortieren der TreeView-Knoten");
        }
    }    
}