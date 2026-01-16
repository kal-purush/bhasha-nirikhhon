import React,{useState, useMemo} from 'react';

export default function Example(){
    const [x, setx]=useState('x coming');
    const [y, sety]=useState('y coming');

   // useState: 多状态声明时
    var youhua= useMemo(()=>cc(x), [x])  //使用useMemo，然后给她传递第二个参数，参数匹配成功，才会执行, 第一个参数为属性改变的操作
    
    function cc(name){
        console.log('shouldCompnentUpdate?')
        return name+', ------>'
    }
    return (
        <div>-----------------
            <h1>useMemo优化性能 </h1>
            <div>function形式组件失去了shouldCompnentUpdate周期, 也就是说我们没有办法通过组件更新前条件来决定组件是否更新。而且在函数组件中，也不再区分mount和update两个状态，这意味着函数组件的每一次调用都会执行内部的所有逻辑，就带来了非常大的性能损耗</div>
            <button onClick={()=>{setx(new Date().getTime())}}>x</button>
            <button onClick={()=>{sety(new Date().getTime())}}>y</button>
            <div>{youhua}</div>
        </div>
    )
}