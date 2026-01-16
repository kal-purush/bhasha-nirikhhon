let  {createWorker} =require('tesseract.js');

const worker = createWorker({
  logger: m => console.log(m)
});

module.exports=(router)=>{
    router.get("/",(req,res,next)=>{
       return res.send("<h1>Testing page</h1>");

    })
    router.get("/scanImage",async (req,res,next)=>{
        await worker.load();
        await worker.loadLanguage('eng');
        await worker.initialize('eng');
        const { data: { text } } = await worker.recognize('ocr-test.jpg');
        console.log(text);
        await worker.terminate();
    })
    return router;
}