#!/usr/bin/env pybricks-micropython
import sys
sys.path.append("./")

# pybricksのreferenceはここ: https://docs.pybricks.com/en/stable/index.html
from pybricks.hubs import EV3Brick
from pybricks.ev3devices import (Motor, TouchSensor, ColorSensor,
                                 InfraredSensor, UltrasonicSensor, GyroSensor)
from pybricks.parameters import Port, Stop, Direction, Button, Color
from pybricks.tools import wait, StopWatch, DataLog
from pybricks.robotics import DriveBase
from pybricks.media.ev3dev import SoundFile, ImageFile
from color import RGBColor
from main2 import LineTraceCar

# EV3の固有デバイス初期化
leftMotor = Motor(Port.C)
rightMotor = Motor(Port.B)
sonicSensor = UltrasonicSensor(Port.S4)

class LineTraceCar2(LineTraceCar):
  def trace(self):
    """ライントレースのメイン処理"""
    # RGBColorクラスの初期化
    rgbColor = RGBColor()
    self.__initMotor()

    flag = self.FLAG_NORMAL
    speed = self.SPEED
    garageColor = Color.YELLOW

    # ラインをトレースして走る
    while True:
      # 色の取得と判定
      gotColor = rgbColor.getColor()

      if gotColor is Color.BLACK:
        if flag == self.FLAG_GREEN:
          self.__initMotor()

          # 緑の後に黒を検出したら90°コーナーを曲がる
          self.__turnLastCurve()
          garageColor = __getGarageColor()
          flag = self.FLAG_NORMAL
          # あとは直線なので、高速度設定
          speed = self.SPEED3

        else:
          # 右旋回
          self.__run(speed[1], speed[0])

      elif gotColor is Color.WHITE: # 白
        # 左回転
      	self.__run(speed[0], speed[1])

      elif gotColor is Color.GREEN: # 緑
        self.__run(speed[1], speed[0])
        flag = self.FLAG_GREEN

      elif gotColor is Color.BLUE and garageColor is Color.BLUE: # 青
        break

      elif gotColor is Color.RED and garageColor is Color.RED: # 赤
        break

      elif gotColor is Color.YELLOW:
        break

      else:
        # 白以外のその他の色も右回転
      	self.__run(speed[1], speed[0])
    # end of while

    # モーターを停止
    leftMotor.stop()
    rightMotor.stop()
    print("trace MotorStop")

  def __getGarageColor(self):
    dist = sonicSensor.distance()
    if dist < 100:
      return Color.BLUE
    elif dist < 150:
      return Color.YELLOW
    else
      return Color.RED

if __name__ == "__main__":
  car = LineTraceCar2()

  # start処理
  car.waitStart(50, 200)

  # ライントレース開始
  car.trace()

  # 駐車する
  car.garageIn()