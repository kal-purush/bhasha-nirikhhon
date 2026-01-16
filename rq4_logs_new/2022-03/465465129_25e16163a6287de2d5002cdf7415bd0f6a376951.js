exports.handler = async (event, context) => {
    const guides = [
        { 
            id: 1, 
            title: "Beat all Zelda Bosses like a Boss", 
            author: "mario",
            desc: "Lorem ipsum dolor, sit amet consectetur adipisicing elit. Reiciendis dolorem et molestias quis. Quia, obcaecati. Dolorem nisi quae veniam blanditiis dolores error quisquam. Quaerat tempora eum perferendis, vitae consequatur rerum."
        },
        { 
            id: 2, 
            title: "Mario Kart shortcuts you never knew existed", 
            author: "luigi",
            desc: "Lorem ipsum dolor, sit amet consectetur adipisicing elit. Reiciendis dolorem et molestias quis. Quia, obcaecati. Dolorem nisi quae veniam blanditiis dolores error quisquam. Quaerat tempora eum perferendis, vitae consequatur rerum"
        },
        { 
            id: 3, 
            title: "Ultimate street fighter", 
            author: "chun-li",
            desc: "Lorem ipsum dolor, sit amet consectetur adipisicing elit. Reiciendis dolorem et molestias quis. Quia, obcaecati. Dolorem nisi quae veniam blanditiis dolores error quisquam. Quaerat tempora eum perferendis, vitae consequatur rerum"
        },    
    ]

    let res
    if (context.clientContext.user) {
        res = JSON.stringify(guides)
        return {
            statusCode: 200,
            body: res,
        }
    }
    res = JSON.stringify({ msg: "You have to be logged in to view this section" })
    return {
        statusCode: 401,
        body: res,
    }
}