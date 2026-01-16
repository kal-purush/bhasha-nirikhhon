const { Users, Thoughts } = require('../models');
const usersController = {
    createUsers ({body}, res){
        Users.create(body)
            .then(dbUsersData => res. json(dbUsersData))
            .catch(err => res.status(400).json(err));
    },
    
    getAllUsers(reg, res){
        Users.find ({})

        .populate({
            path: 'thoughts',
            select: '-_v'
        })
        
        .populate({
            path:'friends',
            select:'-_v'
        })

        .select('-_v')
            .then(dbUsersData => res.json(dbUsersData))
            .catch(err => {
                console.log (err);
                res.status(404).json(err);
            });
        },
        getAllUsers({params},res){
            Users.findOne({_id: params.id})

            .populate({
                path:'thoughts',
                select: '-_v'
            })
            .populate({
                path: 'friends',
                select: '-_v'
            })
            .select('-_v')

             .then(dbUsersData => {
                 if(!dbUsersData){

                     res.status(404).json({message:'No user was found with this id'});
                     return;
                 }
                 res.json(dbUsersData)
            })
            .catch(err => {
                console.log(err);
                res.status(404).json(err);
            });
        },

        updateUsers({params,body}, res){
            Users.findOneAndUpdate({_id: params.id}, body,{new: true,
            runValidators: true})

            .then(dbUsersData => {
                if(!dbUsersData){
                    res.status(404).json({message:'No user was found with this id'});
                    return;
                }
            })
            .catch(err =>{
                console.log(err);
                res.status(400).json(err);
            });
        },

        deleteUsers({ params}, res){
            Users.findOneAndDelete({ _id: params.id })
            .then(dbUsersData => {
                if (!dbUsersData) {
                    res.status(404).json({ message: 'No user was found with this id'});
                    return;
                }
                Users.updateMany(
                    { _id :{$in: dbUsersData.friends} },
                    {$pull: { friends: params.id}  }
                )
                .then(()=> {
                    Thoughts.deleteMany({ username : dbUsersData.username})
                    .then(()=> { 
                        res.json({message: "username was deleted, his/her thoughts & dropped from friends"});
                    })
                    .catch(err => res.status(400).json(err));
                })
                .catch(err => res.status(400).json(err));
            })
            .catch(err => res.status(400).json(err));
        },

        addFriend({params},res){
            Users.findOneAndUpdate({_id: params.id}, {$push:{ friends: params, friendId}},{new:true})
                .populate({
                    path:'friends',
                    select:'-__v'
                })
                .select('-__v')
                .then(dbUsersData => {
                    if (!dbUsersData){
                        res.status(404).json({message:'No user found with this id'});
                        return;
                    }
                    res.json(dbUsersData);
                })
                .catch(err => res.json(err));
        },

        deleteFriend({ params},res){
            Users.findOneAndUpdate({_id: params.id}, {$pull:{ friends: params, friendId}},{new: true})
                .populate({
                    path:'friends',
                    select: '-__v'
                })
                .select('-__v')
                .then(dbUsersData => {
                    if(!dbUsersData){
                        res.status(404).json({message:'No user found with this id'});
                        return;
                    }
                    res.json(dbUsersData);
                })
                .catch(err => res.status(400).json(err));    
        }

};

module.exports = usersController;