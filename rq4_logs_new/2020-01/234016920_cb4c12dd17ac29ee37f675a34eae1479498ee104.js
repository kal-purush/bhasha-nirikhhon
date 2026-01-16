

/*
* 自定义Promise构造函数
* excutor:执行器函数
* */
(function (window) {

    const PENDING = 'pending'
    const RESOLVED = 'resolved'
    const REJECTED = 'rejected'

    function Promise(excutor) {
        //将当前promise对象保存起来
        let self = this
        self.status = PENDING  //给promise对象指定status属性，初始值为PENDING
        self.data = undefined  //给promise对象指定一个用于存储结果数据的属性
        self.callbacks = [] //每个元素的结构：{onRESOLVED(){},onREJECTED()}

        function resolve(value) {
            //如果状态不是PENDING，直接结束
            if (self.status != PENDING) {
                return
            }

            //将状态改变为RESOLVED
            self.status = RESOLVED

            //保存value数据
            self.data = value

            //如果有待执行的callback函数，立即异步执行回调函数onRESOLVED
            if (self.callbacks.length > 0){
                setTimeout(() => {  //放入队列中执行所有成功的回调
                    self.callbacks.forEach(callbasObj => {
                        callbasObj.onResolved(value)
                    })
                })
            }
        }

        function reject(reason) {
            //如果状态不是PENDING，直接结束
            if (self.status != PENDING) {
                return
            }
            //将状态改变为REJECTED
            self.status = REJECTED
            //保存value数据
            self.data = reason
            //如果有待执行的callback函数，立即异步执行回调函数onREJECTED
            if (self.callbacks.length > 0){
                setTimeout(() => {  //放入队列中执行所有成功的回调
                    self.callbacks.forEach(callbasObj => {
                        callbasObj.onRejected(reason)

                    })
                })
            }

        }
        //立即同步执行excutor
        try{
            excutor(resolve,reject)

        }catch (err) { //如果执行器抛出异常，promise对象变为REJECTED状态
            reject(err)
        }




    }
    /*
    * Promise原型对象的then方法
    * 指定成功和失败的回调函数
    * 返回一个新的promise对象
    * */

    Promise.prototype.then=function(onResolved,onRejected){

        onResolved =  typeof onResolved === 'function' ? onResolved : value => value //向后专递成功的value
        //指定默认失败的问题，实现错误、异常穿透的关键点
        onRejected =  typeof onRejected === 'function' ? onRejected : reason => {throw reason} //向后传递失败的reason

        let self = this

        return new Promise((resolve,reject) => {

            //调用指定函数回调处理，根据执行结果，改变return的promise状态
            function handle(callback) {
                /*
                   * 1.如果抛出异常，return的promise就是失败，reason就是err
                   * 2.如果回调函数返回不是promise，return的结果就会成功，value就是返回的值
                   * 3.如果回调函数返回是promise，return的promise结果就是这个promise的结果
                   * */
                try {
                    const result = callback(self.data)
                    //3.如果回调函数返回是promise，return的promise结果就是这个promise的结果
                    if (result instanceof  Promise){
                        /*
                        result.then(
                            value => resolve(value),
                            reason => reject(reason)
                        )
                        */
                        result.then(resolve,reject)
                    } else {
                        //2.如果回调函数返回不是promise，return的结果就会成功，value就是返回的值
                        resolve(result)
                    }
                }catch (err) {
                    //1.如果抛出异常，return的promise就是失败，reason就是err
                    reject(err)
                }
            }

            //当前状态还是PENDING状态，将回调函数保存起来
            if (self.status === PENDING){
                self.callbacks.push({
                    onResolved(value){
                        handle(onResolved)
                    },
                    onRejected(reason){
                        handle(onRejected)
                    }
                })
            } else if (self.status === RESOLVED){
                setTimeout( () => {
                    handle(onResolved)
                    })


            } else { //rejected
                setTimeout(()=>{
                    handle(onRejected)
                    })
            }
        })
    }
    /*
    * Promise原型对象的catch方法
     * 指定失败的回调函数
    * 返回一个新的promise对象
    * */
    Promise.prototype.catch=function(onRejected){
        return this.then(undefined,onRejected)

    }
    /*
    * Promise的resove方法
    * 返回指定结果的成功的promise
    * */
    Promise.resolve=function(value){
        //返回一个成功/失败的promise
        return new  Promise((resolve,reject)=>{
            //value 是promise
            if (value instanceof Promise){  //使用value的结果作为promise的结果
                value.then(resolve,reject)
            } else {
                //value 不是promise，promise变为成功，结果为value
                resolve(value)
            }
        })

    }
    /*
    Promise的reject方法
    返回一个指定reason的失败的promsie
    * */
    Promise.reject=function(reason){
        //返回一个失败的promise
        return new Promise((resolve,reject)=>{
                reject(reason)
            }
        )

    }
    /*
    * Promise的all方法
    * 返回一个promsie，只有所有的promise都成功时才成功，否则只要有一个失败就失败
    * */
    Promise.all=function(promises){
        //
        const values = new Array(promises.length)
        //用来保存成功的promise数量
        let resolvedCount = 0
        return new Promise((resolve,reject)=>{
            //遍历每个promises获取每个promise的结果
            promises.forEach((p,index) => {
                Promise.resolve(p).then(
                    value => {
                        //成功的promise加1
                        resolvedCount++
                        //p成功将成功的value保存到values中
                        values[index] = value

                        //如果全部成功将return的promise变为成功
                        if (resolvedCount == promises.length){
                            resolve(values)
                        }
                    },
                    reason => { //只要有一个失败，return 的promise就失败
                        reject(reason)
                    }
                )
            })

        })

    }
    /*
    *Promise函数对象的all方法
    * 返回一个promise对象，其结果由第一个完成的promsie决定
    * */
    Promise.race=function(Promises){
        return new Promise((resolve,reject) => {
            Promises.forEach(p=>{
                Promise.resolve(p).then(
                    value => {resolve(value)}, //一旦有成功，将return变成成功
                    reason => {reject(reason)} //一旦失败，将return变为失败

                )
            })
        })

    }

    Promise.resolveDelay = function(value,time){
        return new Promise((resolve,reject) => {
            setTimeout(()=>{
               if (value instanceof Promise){
                   value.then(resolve,reject)
               } else {
                   resolve(value)
               }
            },time)
        })
    }

    Promise.rejectDelay = function(reason,time){
        return new Promise((resolve,reject) => {
            setTimeout(()=>{
                reject(reason)
            },time)
        })
    }

    //向外暴露Promise对象
    window.Promise = Promise;
})(window)