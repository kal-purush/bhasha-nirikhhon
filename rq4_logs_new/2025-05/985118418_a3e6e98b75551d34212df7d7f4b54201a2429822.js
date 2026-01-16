// @name 示例插件
// @version 1.0.0

// 添加自定义按钮
api.addButton('导出名单', function() {
    const content = students.join('\n');
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    window.open(url);
});

// 监听抽选事件
window.plugin_示例插件_afterDraw = function(data) {
    api.log(`抽选完成: ${data.results.join(', ')}`);
};