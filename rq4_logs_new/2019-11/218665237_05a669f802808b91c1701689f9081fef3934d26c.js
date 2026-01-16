//1.数据响应化
//2.依赖收集
class Kvue{
    constructor(options){
        this.$options=options;
        this.$data=options.data;
        this.observe(this.$data);
    }
    observe(value){
        if(!value||typeof value !== 'object'){
            return
        }
        Object.keys(value).forEach(key=>{
            //真正的响应化处理
            this.defineReactive(value,key,value[key])
            //代理data中的属性到vue实例上
            this.proxyData(key);
        })
    }
    defineReactive(obj,key,val){
        //递归
        this.observe(val)
        Object.defineProperty(obj,key,{
            get(){
                return val
            },
            set(newVal){
                val=newVal
                console.log(`${key}属性更新了`)
            }
        })
    }
    proxyData(key){
        //需要给vue实例定义属性
        Object.defineProperty(this,key,{
            get(){
                return this.$data[key]
            },
            set(newVal){
                this.$data[key]=newVal
            }
        })
    }
}