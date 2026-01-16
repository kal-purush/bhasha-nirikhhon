export class AppConstant {
    public static getItemTypes() {
        let itemTypes = [{
            "type": "string",
            "title": "文字"
        }, {
            "type": "textarea",
            "title": "文字区域"
        }, {
            "type": "number",
            "title": "数字"
        }, {
            "type": "select",
            "title": "单选列表"
        }, {
            "type": "date",
            "title": "日期"
        }, {
            "type": "time",
            "title": "时间"
        }, {
            "type": "email",
            "title": "Email"
        }, {
            "type": "url",
            "title": "网址"
        }, {
            "type": "toggle",
            "title": "开关"
        }, {
            "type": "password",
            "title": "密码"
        }
        ]
        return itemTypes;
    }

    public static getItemTypeTitle(itemType) {
        let itemTypes = this.getItemTypes();
        for (var i = 0; i < itemTypes.length; i++) {
            if (itemType == itemTypes[i].type) {
                return itemTypes[i].title;
            }
        }
        return "Null";
    }

    public static getItemTypesJson() {
        let itemTypesJson = {
            "string": "文字",
            "textarea": "文字区域",
            "number": "数字",
            "select": "列表",
            "date": "日期",
            "time": "时间",
            "email": "Email",
            "url": "网址",
            "toggle": "开关",
            "password": "密码"
        }
        return itemTypesJson; 
    }
}