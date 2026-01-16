import mongoose from "mongoose";
import {MenuItems} from '../../shared/models/menuItems.model.js'

const { Schema, model } = mongoose

const menuItemSchema = new Schema<MenuItems>({
    name: {
        type:String,required:true
    },
    description: {
        type:String,required:true
    },
    image: {
        type: String,required: true
    },
    images: [{
        type: String,required: true
    }],
    category: [{
        type: mongoose.Types.ObjectId, ref: 'Category'
    }],
    ingredients: [{
        type: mongoose.Types.ObjectId, ref: 'Ingredients'
    }],
    isFeatured:[{type:String,required:true}]   
})

export const MenuItemModel=model('MenuItem',menuItemSchema)