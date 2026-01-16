const express = require("express");
const bodyParser = require("body-parser");
const date = require(__dirname + "/date.js");
const mongoose = require("mongoose");
const _ = require("lodash");


const app = express();

app.set('view engine', 'ejs');
app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static("public"));

mongoose.connect('mongodb://127.0.0.1:27017/todolistDB')

const itemsSchema = new mongoose.Schema({
    name:{
        type: String
    }
});

const Item = mongoose.model('Item',itemsSchema);

const item1 = new Item({
    name:"Welcome to my TodoList"
})

const item2 = new Item({
    name:"Hit the + button to add a new item"
})

const item3 = new Item({
    name:"<-- Hit this to delete an item"
})

const defaultItems =[item1,item2,item3];

const listSchema = {
    name: String,
    items: [itemsSchema]
}

const List = mongoose.model("List",listSchema);

const day = date.getDate();
app.get("/", function(req, res){

    Item.find({})
    .then((items)=>{
        if(items.length === 0){
            Item.insertMany(defaultItems)
            .then((result)=>{
                console.log(result);
            })
            res.redirect("/");
        }else{
            res.render("List", {listTitle: day, newListItems: items});
        }
    })
})

app.get("/:customListName", function(req, res){
    const customListName = _.capitalize(req.params.customListName);

    List.findOne({name:customListName})
    .then((foundList)=>{
        if(foundList){
            //Show an Existing list
            res.render('list',{listTitle: foundList.name, newListItems: foundList.items});
        }else{
            //Creates a new list
            const list = new List({
                name : customListName,
                items: defaultItems
            });
            list.save();
            res.redirect("/" + customListName);
        }
    })
    .catch((err)=>{
        console.error(err);
    })


})

app.post("/", function(req, res){

    const itemName = req.body.newItem;
    const listName = req.body.list;

    const item = new Item({
        name :itemName
    });

    if(listName === day){
        item.save();
        res.redirect("/");
    }else{
        //        Find the right list and add to it
        List.findOne({name:listName})
        .then((foundList)=>{
            foundList.items.push(item);
            foundList.save();
            res.redirect("/" + listName);
        })
    }


});

app.post("/delete", function(req, res){
    const checkedItemId = req.body.checkbox;
    const listName = req.body.listName;

    if(listName === day){
        Item.findByIdAndRemove(checkedItemId)
        .then((result)=>{
            console.log(`Deleted ${result}`);
        })
        .catch((err)=>{
            console.error(`${err}`);
        })
    
        res.redirect("/");
    }else{
        List.findOneAndUpdate({name: listName},{$pull: {items: {_id: checkedItemId}}})
        .then((foundList)=>{
            if(foundList){
                res.redirect("/" + listName);
            }
        })
    }
});

// app.get("/work", function(req, res){
//     res.render("list", {listTitle: "Work List", newListItems: workItems})
// })

// app.post("/work", function(req, res){
//     const item = req.body.newItem;
//     if(item!=""){
//         workItems.push(item);
//     }
//     res.redirect("/work");
// })

// app.get("/about", function(req, res){
//     res.render("about");
// })



app.listen(3000, function(){
    console.log("Server is running on port 3000");
})