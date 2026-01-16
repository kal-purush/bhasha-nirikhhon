const  readline=require('readline');
const  rl=readline.createInterface({
    input: process.stdin,
    output: process.stdout
});


    rl.setPrompt('isming:  ', 'yoshing', 'familya');
    rl.prompt();



rl.on('line', (data1, age, ism)=>{

    let data={
        ismiz: data1,
        yoshingiz: age,
        ism: ism,

    };

    console.log();
    rl.close()
});