const handleBlogRouter = (req, res) => {
    const method = req.method

    // 获取博客列表
    if (method === 'GET' && req.path === '/api/blog/list') {
        return {
            msg: '这是获取博客列表的接口s '
        }
    }

    // 获取博客详情
    if (method === 'GET' && req.path === '/api/blog/detail') {
        return {
            msg: "这是获取一篇博客的内容"
        }
    }

    // 新增博客
    if (method === 'POST' && req.path === '/api/blog/new') {
        return {
            msg: "这是新增一篇博客"
        }
    }
    
    // 更新博客
    if (method === 'POST' && req.path === '/api/blog/update') {
        return {
            msg: "这是更新一篇博客"
        }
    }

    // 删除博客
    if (method === 'POST' && req.path === '/api/blog/del') {
        return {
            msg: "这是删除一篇博客"
        }
    }
}

module.exports = handleBlogRouter