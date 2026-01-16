#!/usr/bin/env python
# encoding: utf-8
'''
@author: Xinming Hou
@license: (C) Copyright 2017-2018, CCNT of Zhejiang Unversity.
@contact: houxinming.chn@foxmail.com
@file: VoiceController.py
@time: 2018/5/16 上午10:54
@description:
    语音提示开始
    循环开始
        发布滴声
        语音识别
        结果判断
        记录日志
        反馈错误或执行命令

'''

import api.AipSpeech.utils.REC as REC
import api.AipSpeech.utils.NLP as NLP
import api.AipSpeech.utils.SpeechPlayer as SpeechPlayer
import yaml

from api.dashgo_interface import *

class voiceController:
    def __init__(self, configs_path, dashgo_serialname):
        # 初始化底盘
        self.dg = dashgo(dashgo_serialname)
        print("start\r 语音控制")
        self.dg.get_length()


        # 导入yaml文件路径
        # 导入参数
        self.configs = yaml.load(open(configs_path))
        # 初始化各个设备

        # 打开翻译器
        self.nlp = NLP.NLP(self.configs["baidu_token"], self.configs["aipSpeech_wav_para"])
        # 打开录音机
        self.rec = REC.recorder(self.configs["device_para"], self.configs["aipSpeech_wav_para"])
        # 打开播放器
        self.player = SpeechPlayer.speechPlayer()


        # 初始化声音模板
        self.voice_template = self.configs["default_wave"]

        self.controller()

    def controller(self):
        self.player.play(self.voice_template["wave_start"])
        while True:
            self.player.play(self.voice_template["wave_di"])

            self.rec.record(self.configs["wave_output_filename"])
            err_cod, result = self.nlp.result(self.configs["wave_output_filename"])
            self.maker(err_cod, result)

    def maker(self, err_cod, result):
        # result = result
        if err_cod == 0:
            if "停" in result:
                print("stop")
                self.player.play(self.voice_template["wave_dgstop"])
                self.dg.stop()
            elif "前" in result:
                print("forward")
                self.player.play(self.voice_template["wave_dgforward"])
                self.dg.go_forward()
            elif "后" in result:
                print("backward")
                self.player.play(self.voice_template["wave_dgback"])
                self.dg.go_forward(False)
            elif "左" in result:
                print("left")
                self.player.play(self.voice_template["wave_dgleft"])
                self.dg.turn(False)
            elif "右" in result:
                print("right")
                self.player.play(self.voice_template["wave_dgright"])
                self.dg.turn()
            elif "关闭" in result:
                print("exit")
                exit()
            else:
                print("command may be wrong")
                self.player.play(self.voice_template["wave_cmdFailed"])
        else:
            print("voice wrong")
            # self.player.play(self.voice_template["wave_voiceFailed"])


if __name__ == '__main__':
    pass
