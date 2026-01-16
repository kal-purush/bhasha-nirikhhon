const uuid = require('uuid')
const path =  require('path')
const  {Application, AppInfo, License} = require('../models/models')
const  ApiError = require('../error/ApiError')

class ApplicationController{
    async create(req,res, next){
        try{
            let  {name, price, developerId, typeId, info, license} = req.body
            const {img} = req.files
            let fileName = uuid.v4()+".jpg"
            img.mv(path.resolve(__dirname, "..", 'static', fileName))
            const  application = await Application.create({name, price, developerId, typeId, img:fileName})
            if (info)
            {
              info = JSON.parse(info)
               info.forEach(i=>
               AppInfo.create({
                   title: i.title,
                   description: i.description,
                   applicationId: application.id
               })
               )
            }

            //  license
          if (license)
            {
                license = JSON.parse(license)
                license.forEach(i=>
                    License.create({
                        DateOfActionId: i.DateOfActionId,
                        IdApp: application.id,
                        VersionId: i.VersionId
                    })
                )
            }

            return res.json(application)
        }
        catch (e){
            next(ApiError.badRequest(e.message))
        }

    }

    async getAll(req,res, ){
 let{developerId, typeId,limit, page} = req.query
        page = page || 1
        limit = limit || 9
        let offset = page*limit-limit
        let application;
 if (!developerId && !typeId)
 {
     application = await  Application.findAndCountAll({limit, offset})
 }

 if (developerId && !typeId)
 {
     application = await  Application.findAndCountAll({where:{developerId}, limit, offset})
 }
 if (!developerId && typeId)
 {
     application = await  Application.findAndCountAll({where:{typeId}, limit, offset})
 }

 if (developerId && typeId)
 {
     application = await  Application.findAndCountAll({where:{typeId, developerId}, limit, offset})
 }

 return res.json(application)
    }


    async getOne(req,res)
    {
        const  {id} = req.params
        const application = await Application.findOne(
            {
                where: {id},
                include: [{model: AppInfo, as: 'info'},

                    {model: License, as: 'license'},
                ]

            }
        )
        return res.json(application)

    }

    async update(req, res) {
        try {
            const {id} = req.params;
            const {developerId, typeId, name, price, info} = req.body;

            await Application.findOne({where:{id}})
                .then( async data => {
                    if(data) {
                        let newVal = {};
                        developerId ? newVal.developerId = developerId : false;
                        typeId ? newVal.typeId = typeId : false;
                        name ? newVal.name = name : false;
                        price ? newVal.price = price : false;

                        if(req.files) {
                            const {img} = req.files;
                            const type = img.mimetype.split('/')[1];
                            let fileName = uuid.v4() + `.${type}`;
                            img.mv(path.resolve(__dirname, '..', 'static', fileName));
                            newVal.img = fileName;
                        }

                        if(info) {
                            const parseInfo = JSON.parse(info);
                            for (const item of parseInfo) {
                                await AppInfo.findOne({where:{id: item.id}}).then( async data => {
                                    if(data) {
                                        await AppInfo.update({
                                            title: item.title,
                                            description: item.description
                                        }, {where:{id: item.id}})
                                    } else {
                                        await AppInfo.create({
                                            title: item.title,
                                            description: item.description,
                                            deviceId: id
                                        })
                                    }
                                })
                            }
                        }

                        await Application.update({
                            ...newVal
                        }, {where:{id}} ).then(() => {
                            return res.json("Приложение обновлено");
                        })
                    } else {
                        return res.json("Это приложение не существует в базе данных");
                    }
                })
        } catch (e) {
            return res.json(e);
        }
    }
}

module.exports = new ApplicationController()