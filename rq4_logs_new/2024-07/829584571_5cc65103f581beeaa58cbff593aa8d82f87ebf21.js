// interview question 

// 1.output based 

console.log("A") ;

setTimeout(()=>{
    console.log('B')
})

// no time is provided i.e default 0  ore functionality of setTimeout callback is wait 

let val = ['C','D']
val.forEach((x)=>{
    console.log(x)
})
console.log('E')

// output  

// A ,C ,D  , E , B


// q.2 

const obj = [
    {
        key :'Sample 1',
        data : 'Data'
    },
    {
        key :'Sample 1',
        data : 'Data'
    },

    {
        key :'Sample 1',
        data : 'Data'
    },
    {
        key :'Sample 2',
        data : 'Data2'
    }, {
        key :'Sample2',
        data : 'Data2'
    }, {
        key :'Sample 1',
        data : 'Data'
    },
    {
        key :'Sample 3',
        data : 'Data3'
    },
]
// output - arrange the same key  

 const output  = {};

 obj.forEach((item)=>{
    if(output[item.key]){
        output[item.key] = [item]
        console.log('key is available')
    }
    else{
        output[item.key] = [item]
    }
 })

 console.log(output)


// 3. why we used Docktypein html  -  to tell the browser which version of html we are using
// 4. wht is use head tag - include meta tag use for sco ,external links , title of document 
// 5. better place of script file in hlml file - just before the closing tag
// can use in head tag also with attribute(differ) in script file which tells the executer hold script and execute after html ready in DOM

// 6. whats is display VALUE OF /type of randomly cretaed element  - Inline(b default element )

// 7.diff. in inline and block 
// can't give witdh and height to inline ,margin can be given L, R not in Top/bottom  ,paddin can be given 
// can give witdh and height ,margin also and every elemnt start from new line