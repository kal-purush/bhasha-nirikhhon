const pathToRegexp = require('path-to-regexp')

module.exports = class Middleware {
  static mockFilter (ctx: any, next: any) {
    console.log(ctx.path)
    // projectId需要是24位 也就是需要mongodb自动生成的长度
    const pathNode = pathToRegexp('/mock/:projectId(.{24})/:mockURL*').exec(ctx.path)
    console.log(pathNode)
    if (!pathNode) ctx.throw(404)
    console.log('初步通过筛选,匹配正常路径成功!')

    ctx.pathNode = {
      projectId: pathNode[1],
      mockURL: '/' + (pathNode[2] || '')
    }

    return next()
  }
}