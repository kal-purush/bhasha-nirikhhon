const IConsltController = require('./IConsltController.js');


const config = require('../config.js');
const ConsltDAO = require('../persistencelayer/dao/'+config.IConsltDAO);
let consltdao = new ConsltDAO();

class ConsltController extends IConsltController{
  constructor(){
    super();
       
  }
  
  async show(req, res)
    {
  
       let conslts = await Consltdao.recovery();
        return res.json(conslts);
    }
  async store(req, res)
     {
        const conslt =  await Consltdao.create(req);
        return res.json(conslt);
     }
   async destroy(req,res){
         let conslt = await Consltdao.delete(req);
         return res.json(conslt);
    }
   async update(req,res){
        let conslt = await Consltdao.update(req);
        return res.json(conslt);
    }

   async index(req,res)
    {
        let conslts = await Consltdao.search(req);
        return res.json(conslts);
    }
  
}
module.exports = ConsltController;