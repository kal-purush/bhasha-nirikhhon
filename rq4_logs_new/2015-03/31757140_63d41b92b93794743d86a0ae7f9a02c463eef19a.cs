using Microsoft.CSharp;
using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;

namespace GeneratingCode
{
    class ConfigCode
    {
        private const string tabstr = "\t";
        private StringBuilder tempsb = new StringBuilder();
        private string outPaht = @".\\ConfigEntity\\Out\\Config.cs";
        private string entityString;
        private Assembly assembly;
        public ConfigCode()
        {
            entityString = Tool.ReadFileToString(@".\\ConfigEntity\\Config\\ConfigString.cs");
            assembly = Tool.LoadAssembly(new CompilerParameters(new string[] { "System.dll", "mscorlib.dll" })
            {
                IncludeDebugInformation = false,
                GenerateExecutable = false,
                GenerateInMemory = true
            }, @".\\ConfigEntity\\ConfigList.cs");
            CodeCompileUnit ccu = new CodeCompileUnit();
            CodeNamespace cn = Tool.NewNameSpace("ConfigData", "System", "System.IO", "System.Text", "System.Collections.Generic", "System.Collections");
            ccu.Namespaces.Add(cn);
            StringBuilder create = new StringBuilder();
            StringBuilder createre = new StringBuilder();
            StringBuilder init = new StringBuilder();
            createre.AppendLine("int i = 0;");
            createre.AppendLine("switch(name){");
            create.AppendLine("ConfigMetaData cmd = null;");
            create.AppendLine("switch(hc){");
            foreach (Type t in assembly.GetTypes())
            {
                Console.WriteLine(t.BaseType);
                if (!t.IsClass || t.IsAbstract || t.BaseType == typeof(Attribute))
                    continue;
                create.AppendLine("case " + t.GetHashCode() + " :");
                create.AppendLine("cmd = new " + t.Name + "();");
                create.AppendLine("break;");

                createre.AppendLine("case \"" + t.Name + "\" :");
                createre.AppendLine("i = "+t.GetHashCode()+";");
                createre.AppendLine("break;");
                CodeTypeDeclaration customerclass = CreateClass(t.Name);
                cn.Types.Add(customerclass);
                customerclass.Members.Add(CreateMethod("Clone", new CodeTypeReference("ConfigMetaData"), "return new " + t.Name + "();"));
                customerclass.Members.Add(CreateMethod("CustomCode", new CodeTypeReference(typeof(int)), "return " + t.GetHashCode()+";"));
                StringBuilder SMethod = new StringBuilder();
                SMethod.AppendLine("");
                StringBuilder DMethod = new StringBuilder();
                customerclass.BaseTypes.Add("ConfigMetaData");
      
                bool isInist = false;
                foreach (FieldInfo fi in t.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                {
                    foreach (Attribute ab in fi.GetCustomAttributes(false))
                    {
                        if (!isInist)
                        {
                            init.AppendLine("foreach(" + t.Name + " temp in ConfigManager.GetList<" + t.Name + ">())");
                            init.AppendLine("{");
                        }
                        isInist = true;
                      FieldInfo myFIFI=  ab.GetType().GetField("FieldName");
                      FieldInfo myFIFI1 = ab.GetType().GetField("FieldName1");
                      string mysfadfs = myFIFI.GetValue(ab).ToString();

                      string mysfadfs1 = myFIFI1.GetValue(ab).ToString();
                      if (fi.FieldType.IsGenericType && fi.FieldType.GetGenericArguments()[0].IsClass && fi.FieldType.GetGenericArguments()[0] !=typeof(string))
                      {
               
             
                          init.AppendLine("temp." + fi.Name + "= ConfigManager.GetList<" + fi.FieldType.GetGenericArguments()[0].Name + ">().FindAll(obj=>obj." + mysfadfs1 + " == temp." + mysfadfs + ");");
                     
                          
                      }
                      else if(fi.FieldType.IsClass || fi.FieldType != typeof(string))
                      {
                          init.AppendLine("temp." + fi.Name + "= ConfigManager.GetList<" + fi.FieldType.Name + ">().Find(obj=>obj." + mysfadfs1 + " == temp." + mysfadfs + ");");
                      }
                      Console.WriteLine(mysfadfs);
                    }
                    CodeMemberField cmf = new CodeMemberField();
                    cmf.Attributes = MemberAttributes.Assembly;
                    cmf.Name = fi.Name;
                    if (fi.FieldType.IsGenericType)
                    {

                        cmf.Type = new CodeTypeReference("List<" + fi.FieldType.GetGenericArguments()[0].Name + ">");
                    }
                    else
                    {

                        cmf.Type = new CodeTypeReference(fi.FieldType.Name);
                    }

                    CodeMemberProperty property = new CodeMemberProperty();
                    string otherFiName= fi.Name.Substring(1);
                    property.Name = UpString(fi.Name);
                    property.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                    property.Type = cmf.Type;
                    property.GetStatements.Add(new CodeMethodReturnStatement(new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), fi.Name)));
                    customerclass.Members.Add(cmf);
                    customerclass.Members.Add(property);
                    if (!isInist)
                    {
                        ReadSerialize(SMethod, fi);
                        ReadDeserialize(DMethod, fi);
                    }
            
                }
                if(isInist)
                init.AppendLine("}");
                customerclass.Members.Add(CreateMethod("Serialize", null, SMethod.ToString(), new CodeParameterDeclarationExpression("BinaryWriter", "bw")));
                customerclass.Members.Add(CreateMethod("Deserialize", null, DMethod.ToString(), new CodeParameterDeclarationExpression("BinaryReader", "br")));
            }
            create.AppendLine("}");
            create.AppendLine("return cmd;");
            createre.AppendLine("}");
            createre.AppendLine("return i;");
            CodeTypeDeclaration ctd = new CodeTypeDeclaration("LoadType");
            ctd.TypeAttributes = TypeAttributes.NestedAssembly;
            CodeMemberMethod ccc = CreateMethod("Get", new CodeTypeReference("ConfigMetaData"), create.ToString(), new CodeParameterDeclarationExpression(typeof(int), "hc"));
            ccc.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
            ctd.Members.Add(ccc);

            CodeMemberMethod ccc1 = CreateMethod("GetCode", new CodeTypeReference(typeof(int)), createre.ToString(), new CodeParameterDeclarationExpression(typeof(string), "name"));
            ccc1.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
            ctd.Members.Add(ccc1);
            if (init.Length > 0)
            {
                Console.WriteLine("");
            }
            CodeMemberMethod ccc13333333 = CreateMethod("Init", null, init.ToString());
            ccc13333333.Attributes = MemberAttributes.Assembly | MemberAttributes.Static;
            ctd.Members.Add(ccc13333333);
            cn.Types.Add(ctd);
            StreamWriter sw = new StreamWriter(outPaht);
            new CSharpCodeProvider().GenerateCodeFromCompileUnit(ccu, sw, new CodeGeneratorOptions()
            {
                BracingStyle = "C",
                BlankLinesBetweenMembers = true,
            });
            sw.Flush();
            sw.Close();

            StreamReader sr = new StreamReader(outPaht);
            string mydata = sr.ReadToEnd();
            sr.Close();
            mydata = mydata.Insert(mydata.LastIndexOf('}'), entityString);

            StreamWriter sw1 = new StreamWriter(outPaht);
            sw1.Write(mydata);
            sw1.Flush();
            sw1.Close();

            CSharpCodeProvider provider = new CSharpCodeProvider();
            CompilerParameters cp = new CompilerParameters(new string[] { "mscorlib.dll", "System.Data.dll" }, "HWQConfigData.dll", false);
            cp.GenerateExecutable = false;
            CompilerResults cr = provider.CompileAssemblyFromSource(cp, mydata);
            foreach (CompilerError ce in cr.Errors)
            {
                Console.WriteLine(ce.ErrorText+"<------ConfigCode>");
                // MessageBox.Show(ce.ErrorText);
            }
        }

        private string UpString(string s)
        {
            string other = s.Substring(1);
            return s.Substring(0, 1).ToUpper() + other;
        }

        private CodeTypeDeclaration CreateClass(string name)
        {
            CodeTypeDeclaration c = new CodeTypeDeclaration(name);
            c.IsClass = true;
            c.TypeAttributes = TypeAttributes.Public;
            return c;
        }

        private CodeMemberMethod CreateMethod(string mehtodName, CodeTypeReference returnType, string mehtodStatement, params CodeParameterDeclarationExpression[] plist)
        {
            CodeMemberMethod c = new CodeMemberMethod();
            c.Name = mehtodName;
            c.Attributes = MemberAttributes.Override | MemberAttributes.Assembly;
            if(returnType !=null)
            c.ReturnType = returnType;
            c.Statements.Add(new CodeSnippetStatement(mehtodStatement));
            c.Parameters.AddRange(plist);
            return c;
        }

        private void ReadSerialize(StringBuilder sb, FieldInfo fi)
        {
            if (fi.FieldType.IsGenericType)
            {
                spacejoin(sb, "if(" + fi.Name + " == null)");
                tempsb.Length = 0;
                spacejoin(tempsb, "bw.Write((short)0);", 4);
                FlowerSlogan(sb, tempsb);
                spacejoin(sb, "else");
                FlowerSloganLeft(sb);
                spacejoin(sb, "bw.Write((short)" + fi.Name + ".Count);", 4);
                spacejoin(sb, "foreach (" + fi.FieldType.GetGenericArguments()[0].Name + " temp in " + fi.Name + ")",4);
                tempsb.Length = 0;

                if (fi.FieldType.GetGenericArguments()[0].IsClass && fi.FieldType.GetGenericArguments()[0] != typeof(String))
                {
                    spacejoin(tempsb, "temp.Serialize(bw);", 3);
                }
                else
                {
                    spacejoin(tempsb, "bw.Write(temp);", 3);
                }
                FlowerSlogan(sb, tempsb);
                FlowerSloganRigth(sb);
            }
            else if (fi.FieldType == typeof(String))
            {
                spacejoin(sb, "bw.Write(" + fi.Name + ");");
            }
            else if (fi.FieldType.IsClass)
            {
                spacejoin(sb, "if(" + fi.Name + "!=null)");
                tempsb.Length = 0;
                spacejoin(tempsb, "bw.Write(true);", 3);
                spacejoin(tempsb, fi.Name + ".Serialize(bw);", 3);
                FlowerSlogan(sb, tempsb);
                spacejoin(sb, "else");
                tempsb.Length = 0;
                spacejoin(tempsb, "bw.Write(false);", 3);
                FlowerSlogan(sb, tempsb);
            }
            else if (fi.FieldType == typeof(DateTime))
            {
                spacejoin(sb, "bw.Write(" + fi.Name + ".ToBinary());");
            }
            else
            {
                spacejoin(sb, "bw.Write(" + fi.Name + ");");
            }
        }

        

        private void ReadDeserialize(StringBuilder sb, FieldInfo fi)
        {
            if (fi.FieldType.IsGenericType)
            {

                spacejoin(sb, "int " + fi.Name + "Count = br.ReadInt16();");
                spacejoin(sb, fi.Name + " = new List<" + fi.FieldType.GetGenericArguments()[0].Name + ">();");
                spacejoin(sb, "for(int i = 0;i<" + fi.Name + "Count;i++)");
                FlowerSloganLeft(sb);
                if (fi.FieldType.GetGenericArguments()[0].IsClass && fi.FieldType.GetGenericArguments()[0] != typeof(string))
                {
                    spacejoin(sb, "if (br.ReadBoolean())");
                    tempsb.Length = 0;
                    spacejoin(tempsb, fi.FieldType.GetGenericArguments()[0].Name + "  obj = new " + fi.FieldType.GetGenericArguments()[0].Name + "();", 3);
                    spacejoin(tempsb, "obj.Deserialize(br);", 3);
                    spacejoin(tempsb, fi.Name + ".Add(obj);", 3);
                    FlowerSlogan(sb, tempsb);
                }
                else
                {
                    spacejoin(sb, fi.Name + ".Add(br.Read" + fi.FieldType.GetGenericArguments()[0].Name + "());", 3);
                }
                FlowerSloganRigth(sb);
            }
            else if (fi.FieldType == typeof(DateTime))
            {
                spacejoin(sb, fi.Name + " =DateTime.FromBinary(br.ReadInt64());");
            }
            else if (fi.FieldType == typeof(String))
            {
                spacejoin(sb, fi.Name + " = br.ReadString();");
            }
            else if (fi.FieldType.IsClass)
            {
                spacejoin(sb, "if(br.ReadBoolean())");
                tempsb.Length = 0;
                spacejoin(tempsb, fi.Name + " = new " + fi.FieldType.Name + "();", 3);
                spacejoin(tempsb, fi.Name + ".Deserialize(br);", 4);
                FlowerSlogan(sb, tempsb);
            }
            else
            {
                spacejoin(sb, Primary(fi.Name, fi.FieldType.Name));
            }
        }

        private string Primary(string fieldName, string TypeName)
        {
            return fieldName + " = br.Read" + TypeName + "();";
        }


        private void spacejoin(StringBuilder sb, string text, int count = 3)
        {
            for (int i = 0; i < count; i++)
            {
                sb.Append(tabstr);
            }
            sb.AppendLine(text);
        }

        private void FlowerSlogan(StringBuilder sb, StringBuilder tempsb)
        {

            FlowerSloganLeft(sb);
            sb.Append(tempsb.ToString());
            FlowerSloganRigth(sb);
        }

        /// <summary>
        /// 左边花括号
        /// </summary>
        /// <param name="sb"></param>
        private void FlowerSloganLeft(StringBuilder sb)
        {
            sb.Append(tabstr + tabstr + tabstr);
            sb.AppendLine("{");
        }

        /// <summary>
        /// 右边花括号
        /// </summary>
        /// <param name="sb"></param>
        private void FlowerSloganRigth(StringBuilder sb)
        {
            sb.Append(tabstr + tabstr + tabstr);
            sb.AppendLine("}");
        }
    }
}