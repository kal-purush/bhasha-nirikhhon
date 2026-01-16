using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks;
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
namespace TestProject
{
    public class Algorithm
    {
        //0
        //wrapper function-call to the next steps

        //1
        //Validation checks to the query
        public static bool Validation(string query)
        {
            if (query == null)
                return false;
            var diagnostics = KustoCode.ParseAndAnalyze(query).GetDiagnostics();
            return diagnostics.Count == 0;
        }

        //2
        /// <summary>
        /// pass the query, find the sensitve code.
        /// </summary>
        /// <param name="query">a Kusto query</param>
        /// <param name="CustomerDataWords">list of all customer data had found</param>
        /// <param name="code">Convert the query in order to prase it</param>
        /// <returns>list of all customer data had found</returns>
        public static List<string> PassQueryFindCustomerData(string query)
        {
            List<string> CustomerDataWords = new List<string>();
            var code = KustoCode.Parse(query);
            SyntaxElement.WalkNodes(code.Syntax,
                n =>
                {
                    {
                        switch (n.Kind)
                        {
                            //Sensitive Operators-might contain Customer Data
                            //each Node operator represents root of tree, the first Descendant is the Customer Data word.

                            case SyntaxKind.ExtendOperator:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;
                            case SyntaxKind.LetStatement:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;
                            case SyntaxKind.LookupOperator:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;
                            case SyntaxKind.SummarizeOperator:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;
                            case SyntaxKind.ProjectRenameOperator:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;
                            case SyntaxKind.AsOperator:
                                CustomerDataWords.Add(n.GetFirstDescendant<NameDeclaration>().ToString());
                                break;


                            //Sensitive Parmeters-themselvs Customer Data word.

                            case SyntaxKind.NamedParameter:
                                CustomerDataWords.Add(n.ToString());
                                break;
                            case SyntaxKind.StringLiteralExpression:
                                CustomerDataWords.Add(n.ToString());
                                break;
                            case SyntaxKind.FunctionCallExpression:
                                //check if a function declaration called a customer data
                                CustomerDataWords.Add(n.ToString());
                                break;
                            case SyntaxKind.SkippedTokens:
                                //to check 
                                break;
                        }
                    }
                });
            return CustomerDataWords;
        }
        //4
        //Replace the customer data words 

        //5
        //return the clean query
    }
}