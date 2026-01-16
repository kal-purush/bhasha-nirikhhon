module.exports = {
    schedule: {
        // 周一的3点30分30秒更新缓存
        corn: '0 0 0 * * *',
        //   interval: 7*24*60*60*1000,
        type: 'all', // 指定所有的 worker 都需要执行
    },
    async task(ctx) {
        try{
            await ctx.service.data.insertKOLInfo();
        }catch(err){
            console.log(err);
        }
    },
  };