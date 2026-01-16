using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dev13
{
    class FirstCriterion : ICriterion
    {
        public int[] SelectTeam(int[][] paramsOfProgrrammers, int productivity, int budget)
        {
            double[] result = new double [ paramsOfProgrrammers.GetLength ( 0 ) ];

            double[,] arrayForSimplex = FormArrayForSimplex(paramsOfProgrrammers, budget );

            Simplex mySimplex = new Simplex(arrayForSimplex);
            arrayForSimplex = mySimplex.Calculate(result);

            int[] resInt = new int [ paramsOfProgrrammers.GetLength ( 0 ) ];
            for (int i = 0; i < result.Length; i++)
            {
                result [ i ] = Convert.ToInt32 ( result [ i ]);
            }

            return resInt;
        }

        private double[,] FormArrayForSimplex ( int[][] paramsOfProgrrammers , int budget )
        {
            int sizeLine = 2;
            int sizeColumn = paramsOfProgrrammers.GetLength(0) + 1 ;
            double[,] result = new double [ sizeLine , sizeColumn ];

            for ( int i = 1 ; i < sizeColumn ; i++ )
            {
                result [ 0 , i ] = paramsOfProgrrammers [ i - 1 ][ 1 ];
            }

            for ( int i = 1 ; i < sizeColumn ; i++ )
            {
                result [ 1 , i ] = -1 * paramsOfProgrrammers [ i - 1 ][ 0 ];
            }

            result [ 0 , 0 ] = budget ;

            return result;
        }

    }
}