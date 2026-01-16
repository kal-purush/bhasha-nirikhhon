$(function() {
            var layer = layui.layer
            var form = layui.form
            initArtCateList()

            function initArtCateList() {
                $.ajax({
                    method: 'GET',
                    url: '/my/article/cates',
                    success: function(res) {
                        var htmlStr = templete('tpl-table', res)
                        $('tbody').html(htmlStr)
                    }
                })
            }
            var addIndex = null;
            $('#btnAddCate').on('click', function() {
                addIndex = layer.open({
                    type: 1,
                    area: ['500px', '250px'],
                    title: '添加文章分类',
                    content: $('#dialog-add').html()
                });
            })

            $(body).on('submit', '#form-add', function(e) {
                e.preventDefault();
                $.ajax({
                    method: 'POST',
                    url: '/my/article/addcates',
                    data: $(this).serialize(),
                    success: function(res) {
                        if (res.status !== 0) {
                            return layer.msg('新增分类失败！')
                        }
                        layer.msg('新增分类成功！')
                        initArtCateList()
                        layer.close(addIndex);
                    }
                })
            })


            var editIndex = null;
            $('tbody').on('submit', '.btn-edit', function() {
                editIndex = layer.open({
                    type: 1,
                    area: ['500px', '250px'],
                    title: '修改文章分类',
                    content: $('#dialog-edit').html()
                });
                var id = $(this).attr('data-id');
                $.ajax({
                    method: 'GET',
                    url: '/my/article/cates/' + id,
                    success: function(res) {
                        form.val('form-edit', res.data)
                    }
                })
            })

            $('body').on('submit', '#form-edit', function(e) {
                e.preventDefault();
                $.ajax({
                    method: 'POST',
                    url: '/my/article/updatecate',
                    data: $(this).serialize(),
                    success: function(res) {
                        if (res.status !== 0) {
                            return layer.msg('更新分类数据失败！')
                        }
                        layer.msg('更新分类数据成功！')
                        layer.close(editIndex)
                        initArtCateList()
                    }
                })
            })

            $('body').on('submit', '.btn-delete', function() {
                var id = $(this).attr('data-id');
                layer.confirm('确认删除?', { icon: 3, title: '提示' }, function(index) {
                    $.ajax({
                        method: 'GET',
                        url: '/my/article/deletecate/' + id,
                        success: function(res) {
                            if (res.status !== 0) {
                                return layer.msg('删除分类失败！')
                            }
                            layer.msg('删除分类成功！')
                            layer.close(index)
                            initArtCateList()
                        }
                    })
                })

            })