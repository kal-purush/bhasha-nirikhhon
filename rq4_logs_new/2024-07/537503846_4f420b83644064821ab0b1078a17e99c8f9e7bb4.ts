import { Context } from "koa";
import { Msg } from "../class";
import { LayoutService } from "../services";

class layoutController {
  async delete(ctx: Context) {
    let msg = new Msg();
    let serial = ctx.request.query.serial;
    if (!serial) {
      msg.setCode(204).setMsg("参数不能为空");
      ctx.body = msg;
      return;
    }
    let result = await LayoutService.deletelayout(serial as any as number);
    msg.setByRestult(result, {
      code: 200,
      msg: "删除成功",
    });
    ctx.body = msg;
  }
  async add(ctx: Context) {
    let msg = new Msg();
    let { name } = ctx.request.body;
    if (!name) {
      msg.setCode(204).setMsg("参数不能为空");
      ctx.body = msg;
      return;
    }
    let result = await LayoutService.addlayout(name);
    msg.setByRestult(result, {
      code: 200,
      msg: "添加成功",
    });
    ctx.body = msg;
  }
  async modify(ctx: Context) {
    let msg = new Msg();
    let { name,serial } = ctx.request.body;
    if (!serial ||!name) {
      msg.setCode(204).setMsg("参数不能为空");
      ctx.body = msg;
      return;
    }
    let result = await LayoutService.modifylayout(serial, name);
    msg.setByRestult(result, {
      code: 200,
      msg: "修改成功",
    });
    ctx.body = msg;
  }
  async get(ctx: Context) {
    let msg = new Msg();
    let result = await LayoutService.getlayouts();
    msg.setByRestult(result, {
      code: 200,
      msg: "查询成功",
    });
    ctx.body = msg;
  }
}

export default new layoutController();