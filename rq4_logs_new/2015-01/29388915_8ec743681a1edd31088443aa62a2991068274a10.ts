
interface ICell {
    content?: string;                           //can be html   
    attributes?: { [key: string]: string; }     //HTML attributes like id, class, style...
}




/*
 * @tag         string              can be either "td" or "th"
 * @definition  ICellDefinition     data that defines the table cell
 */
function getCellHtml(tag: string, definition: ICell): string {
    assert(tag);
    definition.attributes = definition.attributes || {};
    var html: string = "<" + tag;



    //    + " class='" + (this.className || "") + "'";
    //+ " id='" + (this.id || "") + "'";
    //+ ">" +
    //+ this.content
    //+ "</" + tag + ">";
    
    return "<td>-</td>";
}


//class JSCell {
//    content: string = "";       //can be html

//    element: JQuery;            //Reference to the <th>/<td>-tag

   



//    constructor(definition?: ICellDefinition) {
//        definition = definition || {};
        
//        //Developer note: If the number of attributes rises, the following approach can be used as well:
//        //  var members = ["content", "id", "className"];
//        //  for (var m in members) {
//        //      this[m] = definition[m] || "";
//        //  }

//        this.content = definition.content || "noContent";       //Default content
//        this.id = definition.id || "";
//        this.className = definition.className || "";
//    }


//    /*
//     * @tag     string      can be either "td" or "th"
//     */
//    getHtml(tag: string): string{

//        //TODO: wie soll ich das element-attr. befüllen?
//        //--> "getHtml" in "appendToDom" umbenennen?
//        return "<" + tag
//                + " class='" + (this.className || "") + "'";
//                + " id='" + (this.id || "") + "'";
//            + ">" +
//               + this.content
//            + "</" + tag + ">";
//    }

//}