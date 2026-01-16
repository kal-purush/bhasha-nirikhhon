const Bookmark = require('../../models/other/bookmarkModel')



module.exports.addToBookmark = async (req, res) => {


    const { bookmarkData, userId } = req.body

    // console.log(bookmarkData);

    try {

        const result = await Bookmark.updateOne(
            { user: userId },
            { $push: { items: bookmarkData } },
            { upsert: true }
        );


        if (result) {

            return res.status(200).json('Item added succsffuly')
        }

        return res.status(400).json('Somthing went wrong')

    } catch (error) {

        return res.status(400).json(error.message)

    }
}



module.exports.checkItemIntheBookmark = async (req, res) => {



    const id = req.user.id

    const prductID = req.params.id



    try {
        const result = await Bookmark.findOne(
            { user: id, items: { $elemMatch: { product: prductID } } }, // Query to match items.product
            { items: { $elemMatch: { product: prductID } } } // Projection to include only the matching item
        );

        // console.log(result);



        if (result) {

            return res.status(200).json(result.items[0])
        }

        return res.status(400).json('Somthing went wrong')

    } catch (error) {

        return res.status(400).json(error.message)

    }
}




module.exports.removeBookmarkItme = async (req, res) => {

    const id = req.user.id

    const prductID = req.params.id



    try {
        const result = await Bookmark.updateOne(
            { user: id },
            { $pull: { items: { product: prductID } } })

        // console.log(result); 

        if (result) {

            return res.status(200).json('Item removed successfully')
        }



        return res.status(400).json('Somthing went wrong')

    } catch (error) {

        return res.status(400).json(error.message)

    }
}




module.exports.getBookmarkItems = async (req, res) => {

    // console.log('afd');
    

    const id = req.user.id

    try {
        const result = await Bookmark.findOne( { user: id }, {}).populate({
            path: 'items.product', // Populate the product field
            populate: { path: 'category',select: 'name' }, // Then populate the category field within product
          })

        // console.log(result); 

        if(result.length<=0){

            return res.status(400).json('no Data')
        }

        if (result) {

            return res.status(200).json(result)
        }


        return res.status(400).json('Somthing went wrong')


    } catch (error) {

        return res.status(400).json(error.message)

    }
}