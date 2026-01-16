/*!
 * @file colinTemplate.js
 * @brief a template engine which is used in web front end.
 * @author colinZhang
 * @date 2018/08/14
 * @version 1.0.0
 * @function render by 1.contentObj[keyName] 2.contentObj[keyName1][keyName2] 3.array loop engine
 */

function rachelTemplate(){

    //主要的函数,用户将未填充的模板进行填充
    this.renderTem=function(temp,contentObj){
        return this.insertTemp(temp,contentObj)
    }

    function getAllTemp(temp,contentObj){
        //temp_last获取模板第一个 `}. 以此判断是否需要递归该函数
        var temp_last=temp.search(/`}/)
        if(temp_last<0){
            return temp
        }

        else{
            //获取temp_last前面的所有字符串
            var temp_last_str=temp.substring(0,temp_last)
            //判断是否是循环
            if(temp_last_str.indexOf('#')>0){
                //是循环,谨记  返回的是这次分割后的字符串
                //应该分为三步，一是获取循环块前的String,二是获取循环中的String,三是获取循环后的模板中是String
                //获取循环块前的String
                var first_temp=temp.substring(0,temp.search(/{`/))
                //获取循环中的String,需要先获取到后面的{`#`},就要
                //temp_last_last指的是模板中第一个`}后面的模板String
                var temp_last_last=temp.substring(temp_last+2)
                //xunhuan_temp就是整个循环中的循环块
                var xunhuan_temp=temp_last_last.substring(0,temp_last_last.search(/{`#/))
                var last_temp=temp_last_last.substring(temp_last_last.search(/#`}/)+3)
                //还需要得到循环关键变量
                var xunhuan_key=temp_last_str.substring(temp_last_str.search(/#/)+1)
                var all_xunhuan_str=""

                for(var i=0;i<contentObj[xunhuan_key].length;i+=1){
                    var xunhuan_str=""
                    var xunhuan_cody_temp=xunhuan_temp

                    xunhuan_str=usualInsert(xunhuan_cody_temp,contentObj[xunhuan_key][i])
                    all_xunhuan_str+=xunhuan_str
                }
                //得到temp  ->  first_temp+all_xunhuan_str+last_temp
                temp=first_temp+all_xunhuan_str+last_temp
            }
            else{
                //不是循环
                //就需要区分有没有点
                //也需要first_temp,last_temp
                var first_temp=temp.substring(0,temp.search(/{`/))
                var last_temp=temp.substring(temp_last+2)
                var key_content=temp_last_str.substring(temp_last_str.search(/{`/)+2)
                if(key_content.indexOf('.')>0){
                    //含有点
                    //得到点前面和点后面
                    var first_spot=key_content.split('.')[0]
                    var last_spot=key_content.split('.')[1]
                    var content_temp=contentObj[first_spot][last_spot]
                    temp=first_temp+content_temp+last_temp
                }
                else{
                    //没有点
                    temp=first_temp+contentObj[key_content]+last_temp
                }
            }

          return  getAllTemp(temp,contentObj)
        }

    }

    this.insertTemp=function(tem,contentOb){

        return getAllTemp(tem,contentOb)
    }

    function usualInsert(temp,contentObj){
        //该函数为辅助函数，填充没有循环块的模板
        while(true){
            var temp_last=temp.search(/`}/)

            if(temp_last<0){
                break
            }

            else{
                //获取temp前面的temp_first,获取temp后面的temp_last,以及需要替换的temp_center
                var temp_content_first=temp.substring(0,temp.search(/{`/))
                var temp_content_last=temp.substring(temp_last+2)
                var temp_content_center=temp.substring(temp.search(/{`/)+2,temp_last)
                temp_content_center=contentObj[temp_content_center]
                temp=temp_content_first+temp_content_center+temp_content_last
            }
        }
        return temp
    }
}

var rachel=new rachelTemplate()