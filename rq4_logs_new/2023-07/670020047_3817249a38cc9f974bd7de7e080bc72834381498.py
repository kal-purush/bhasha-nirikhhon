import re
import requests
from plugins import register, Plugin, Event, logger, Reply, ReplyType
import base64
import json
import random
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import asyncio
import time
import websockets

@register
class Tts(Plugin):
    name = "tts"
    def did_receive_message(self, event: Event):
        pass

    def will_generate_reply(self, event: Event):
        query = event.context.query
        if query == self.config.get("command"):
            event.reply = self.reply()
            event.bypass()

    def will_send_reply(self, event: Event):
        pass

    def help(self, **kwargs) -> str:
        return "Use the command #porn(or whatever you like set with command field in the config) to get a wonderful video"

    def reply(self) -> Reply:
        reply = Reply(ReplyType.TEXT, "Failed to get porn videos")
        try:
            flag = self.tts_ws()
            if flag ==True:
                reply = Reply(ReplyType.MP3, "mp3")
        except Exception as e:
            logger.error(f"Audio api call error: {e}")
        return reply
    
    async def tts_ws(self):
        result_path = "./output.mp3"
        payload = {
            "audio_config": {
                "bit_rate": 64000,
                "enable_split": False,
                "enable_timestamp": False,
                "format": "mp3",
                "sample_rate": 24000,
                "speech_rate": 0
            },
            "speaker": "BV426_streaming",
            "text": "测试一下啦"
        }
        api_url = "ws://sami.bytedance.com/internal/api/v2/ws?device_id=2412774897234120&iid=3732188851614814"
        req = {
            "appkey": "IZjhUeAYwP",
            "event": "StartTask",
            "namespace": "TTS",
            "payload": json.dumps(payload)
        }
        # try:
        st = time.perf_counter()
        flag = 0
        result_data = open(result_path, "wb+")
        async with websockets.connect(api_url, ping_interval=None) as ws:
            # 先发送开始事件
            await ws.send(json.dumps(req))
            # 然后发送该事件是否发送完成
            req["event"] = "FinishTask"
            first_package_time = None
            await ws.send(json.dumps(req))
            while True:
                res = await ws.recv()
                try:
                    if isinstance(res, str):
                        res_dict = json.loads(res)
                        if "data" in res_dict:
                            if flag == 0:
                                first_package_time = time.perf_counter() - st
                                flag = 1
                            result_data.write(base64.b64decode(res_dict["data"]))
                        if "payload" in res_dict:
                            print(" payload=%s" % res_dict["payload"], end="")
                        print(" task_id=%s, event=%s status_code=%d status_text=%s" % (
                            res_dict["task_id"], res_dict["event"], res_dict["status_code"], res_dict["status_text"]))
                        if res_dict["event"] == "TaskFinished":
                            # await ws.close()
                            break
                    else:
                        print("receive binary message, len=%d" % len(res))
                        result_data.write(res)
                        if flag == 0:
                            first_package_time = time.perf_counter() - st
                            flag = 1
                        # print(res)
                except Exception as e:
                    print("exception", e)
                    break
            if first_package_time is not None:
                print("首包时间：", first_package_time)
        result_data.close()
        return True