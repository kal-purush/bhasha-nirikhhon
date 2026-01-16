var common = {
    /**
    * 生成查询的参数对象
    * @param obj 查询属性对象
    * @returns {*}
    */
    setParameterObj: function (obj) {
        var objPara = new Object();
        objPara.Lbracket = "";
        objPara.Compare = obj.Compare;
        objPara.Field = obj.Field;
        objPara.DataType = obj.DataType;
        objPara.Value = obj.Value;
        objPara.DisplayValue = obj.DisplayValue;
        objPara.Rbracket = "";
        objPara.Relation = "and";
        objPara.IsCanChange = true;
        objPara.ConvertUpperToCompare = false;
        objPara.Expresstype = 0;
        objPara.FieldCaption = "";
        objPara.Owner = "";
        objPara.Description = "";
        return objPara;
    }
}