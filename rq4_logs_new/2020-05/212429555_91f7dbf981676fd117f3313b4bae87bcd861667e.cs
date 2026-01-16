/*
 * This file is part of the Buildings and Habitats object Model (BHoM)
 * Copyright (c) 2015 - 2020, the respective contributors. All rights reserved.
 *
 * Each contributor holds copyright over their respective contributions.
 * The project versioning (Git) records all such contribution source information.
 *                                           
 *                                                                              
 * The BHoM is free software: you can redistribute it and/or modify         
 * it under the terms of the GNU Lesser General Public License as published by  
 * the Free Software Foundation, either version 3.0 of the License, or          
 * (at your option) any later version.                                          
 *                                                                              
 * The BHoM is distributed in the hope that it will be useful,              
 * but WITHOUT ANY WARRANTY; without even the implied warranty of               
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                 
 * GNU Lesser General Public License for more details.                          
 *                                                                            
 * You should have received a copy of the GNU Lesser General Public License     
 * along with this code. If not, see <https://www.gnu.org/licenses/lgpl-3.0.html>.      
 */

using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Collections;
using System.ComponentModel;
using BH.oM.Reflection.Attributes;
using BH.Engine.Serialiser;
using BH.oM.Base;
using BH.Engine.Reflection;
using Microsoft.CSharp.RuntimeBinder;
using UT = BH.oM.Test.UnitTests;
using BH.oM.Reflection.Interface;
using BH.oM.Reflection;

namespace BH.Engine.UnitTest
{
    public static partial class Compute
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("")]
        [Input("", "")]
        [Output("", "")]
        public static Output<List<List<object>>, Dictionary<string, List<string>>> RunUnitTest(this UT.UnitTest test)
        {
                    
            List<List<object>> results = new List<List<object>>();
            Dictionary<string, List<string>> errors = new Dictionary<string, List<string>>();
            MethodBase method = test.Method;
            foreach (UT.TestData data in test.Data)
            {
                object result = null;
                try
                {
                    ParameterInfo[] parameters = method.GetParameters();
                    if (data.Inputs.Count == parameters.Length)
                    {
                        List<object> inputs = new List<object>();
                        for (int i = 0; i < parameters.Length; i++)
                            inputs.Add(CastToType(data.Inputs[i], parameters[i].ParameterType));
                        result = method.Invoke(null, inputs.ToArray());

                        IOutput output = result as IOutput;

                        if (output == null)
                            results.Add(new List<object> { result });
                        else
                        {
                            List<object> outputs = new List<object>();
                            for (int i = 0; i < output.IOutputCount(); i++)
                            {
                                outputs.Add(output.IItem(i));
                            }
                            results.Add(outputs);
                        }
                    }
                    else
                    {
                        AddError(errors, "The number of inputs is not matching the number of parameters", method);
                        continue;
                    }
                }
                catch (Exception e)
                {
                    string message = "Failed to run with the given inputs";
                    if (e is NotImplementedException || e.InnerException is NotImplementedException)
                        message = "The method is not implements";
                    else if (e is RuntimeBinderException || e.InnerException is RuntimeBinderException)
                        message = "The method with the correct input types cannot be found";
                    AddError(errors, message, method);
                    continue;
                }
            }

            return new Output<List<List<object>>, Dictionary<string, List<string>>> { Item1 = results, Item2 = errors };
        }

        /***************************************************/
        /**** private Methods                           ****/
        /***************************************************/


        private static object CastToType(object item, Type type)
        {
            try
            {
                if (type.IsGenericType && item is IEnumerable)
                {
                    dynamic list;
                    if (type.IsInterface)
                        list = Activator.CreateInstance(typeof(List<>).MakeGenericType(type.GenericTypeArguments[0]));
                    else
                        list = Activator.CreateInstance(type);
                    foreach (dynamic val in (IEnumerable)item)
                    {
                        if (val is IEnumerable)
                            list.Add(CastToType(val, type.GenericTypeArguments[0]));
                        else
                            list.Add(val);
                    }
                    return list;
                }
                else
                {
                    return item;
                }
            }
            catch
            {
                return item;
            }
        }

        /***************************************************/

        private static string CreateErrorMessage(Dictionary<string, List<string>> errors)
        {
            string message = "";
            foreach (var kvp in errors.OrderBy(x => x.Key))
            {
                if (kvp.Value.Count > 0)
                {
                    message += "\n- " + kvp.Key;
                    foreach (string typeName in kvp.Value.Distinct())
                        message += "\n\t- " + typeName;
                }
            }

            return message;
        }


        /*******************************************/

        private static void AddError(Dictionary<string, List<string>> errors, string message, MethodBase method)
        {
            if (!errors.ContainsKey(message))
                errors[message] = new List<string>();
            errors[message].Add(method.ToText(true, "(", ",", ")", false));
        }

        /***************************************************/
    }
}