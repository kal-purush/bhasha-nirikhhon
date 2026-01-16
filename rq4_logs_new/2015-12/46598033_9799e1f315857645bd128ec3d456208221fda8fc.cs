using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ebox_client
{
    //加密类
    //功能，1.产生提取码；2.web服务数据传输加解密
    class Encrypt
    {

        public string GenUserCode()
        {
            string Code="";
            int randomNum;
            Random Rand = new Random();
            for(int i=0;i<8;i++)
            {                
                randomNum = Rand.Next(10);
                Code += randomNum.ToString();
            }
            return Code;
        }
    }
}