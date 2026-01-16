window.addEventListener('load', function () {
    let form = layui.form
    // 渲染文章类别选择框
    category()
    // 初始化富文本编辑器
    initEditor()

    // 渲染文章类别选择框
    function category() {
        $.ajax({
            method: 'get',
            url: '/my/article/cates',
            success: function (res) {
                if (res.status !== 0) {
                    return layer.msg('获取文章分类列表失败,请重试', { icon: 5, offset: '5px' })
                }
                // 分类选择框模板
                let a = template('Select', res)
                // console.log(a)
                document.querySelector('.select').innerHTML = a
                // 让layui重新渲染一次
                form.render()
            }
        })
    }
    // 1. 初始化图片裁剪器
    var $image = $('#image')
    // 2. 裁剪选项
    var options = {
        aspectRatio: 400 / 280,
        preview: '.img-preview'
    }
    // 3. 初始化裁剪区域
    $image.cropper(options)
    // 点击选择封面按钮触发文件选择
    $('#btnChooseImage').on('click', function () {
        $('.getimage').click()
    })

    // 将选择的文件放入裁剪区域
    $('.getimage').on('change', function (e) {
        // 获取选择的文件列表
        let files = e.target.files
        // 未选择文件直接退出
        if (files.length === 0) {
            return
        }
        // 选择文件后,将文件渲染到裁剪区
        let a = URL.createObjectURL(files[0])
        $image
            .cropper('destroy') // 销毁旧的裁剪区域
            .attr('src', a) // 重新设置图片路径
            .cropper(options) // 重新初始化裁剪区域
    })

    // 定义发布状态
    let state = '已发布'
    $('.draft').on('click', function () {
        state = '草稿'
    })
    // 表单提交事件
    $('.layui-form').on('submit', function (e) {
        e.preventDefault()
        // 获取FormData数据
        let form = document.querySelector('.layui-form')
        let fm = new FormData(form)
        // 将文章状态添加进去
        fm.append('state', state)
        // 获取裁剪图片
        $image
            .cropper('getCroppedCanvas', {
                // 创建一个 Canvas 画布
                width: 400,
                height: 280
            })
            .toBlob(function (blob) {
                // 将 Canvas 画布上的内容，转化为文件对象
                // 得到文件对象后，进行后续的操作
                // 5. 将文件对象，存储到 fd 中
                fm.append('cover_img', blob)
                add_newArticle(fm)
            })
    })
    // 发送新文章的ajax请求

    function add_newArticle(fm) {
        $.ajax({
            method: 'post',
            url: '/my/article/add',
            data: fm,
            // 使用FormData函数时,必须加入以下对象,固定写法
            contentType: false,
            processData: false,
            success: function (res) {
                if (res.status !== 0) {
                    return layer.msg(res.message, { icon: 5, offset: '5px' })
                }
                layer.msg(res.message, { icon: 6, offset: '5px' })
                // 跳转回文章列表页面
                setTimeout(function () {
                    location.href = '/article-html/article-list.html'
                }, 1000)
            }
        })
    }

})