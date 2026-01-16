import tkinter, random
'''
项目分析:
    - 屏保是由两个模块构成
        - 球: 使用一个类来实现他的大小,颜色,移动轨迹
            - 创建一个球需要满足在屏幕上的任意一个地方定一个点, 以点的位置加上圆的半径画出一个圆
        - 屏幕: 屏幕上需要出现多个球在上面活动
    - 需要使用到tkinter模块进行图形界面显示
    - 使用random随机产生球的位置,大小,个数,颜色
'''

# 球的类
class Ball():

    def __init__(self, canvas, screenwidth, screenheight):
        '''
        构建球的初始属性
        :param canvas: 画布
        :param screenwidth: 屏幕的宽度
        :param screenheight: 屏幕的高度
        '''
        self.canvas = canvas
        self.width = screenwidth
        self.height = screenheight
        c = lambda: random.randint(0,255)
        self.color = '#%02x%02x%02x'%(c(), c(), c())
        self.xpos = random.randint(20, int(screenwidth)-50)
        self.ypos = random.randint(20, int(screenheight)-50)
        self.radius = random.randint(50, 150)
        self.xvelocity = 50
        self.yvelocity = 50

    def create_ball(self):
        '''
        创建一个球, 颜色, 位置都是随机出现的
        :return:
        '''

        x1 = self.xpos - self.radius
        y1 = self.ypos - self.radius

        x2 = self.xpos + self.radius
        y2 = self.ypos + self.radius

        self.item = self.canvas.create_oval(x1,y1,x2,y2, fill=self.color, outline=self.color)

    def move_ball(self):

        '''
        移动球的位置
        :return:
        '''
        self.xpos += self.xvelocity
        self.ypos += self.yvelocity

        # 如果球在撞球的时候则反方向移动
        if self.xpos - self.radius <= 0 or self.xpos + self.radius >= self.width:
            self.xvelocity *= -1
        if self.ypos - self.radius <= 0 or self.ypos + self.radius >= self.height:
            self.yvelocity *= -1

        self.canvas.move(self.item, self.xvelocity, self.yvelocity)

class Screen_Saver():
    '''
    屏保
    '''
    # 创建球的列表
    balls = list()
    def __init__(self):
        self.num_ball = random.randint(5,12)
        self.baseFrame = tkinter.Tk()
        self.baseFrame.wm_title('屏保')
        self.baseFrame.overrideredirect(1)
        self.button1 = tkinter.Button(self.baseFrame, text='点击打开屏保', command=self.screen)
        self.button1.pack()

        self.baseFrame.mainloop()

    def screen(self):
        self.button1.destroy()
        w, h = self.baseFrame.winfo_screenwidth(), self.baseFrame.winfo_screenheight()
        self.canvas = tkinter.Canvas(self.baseFrame, width=w, height=h)
        self.canvas.pack()

        for i in range(self.num_ball):
            ball = Ball(self.canvas, w, h)
            ball.create_ball()
            self.balls.append(ball)

        self.baseFrame.bind('<Motion>', self.myquit)
        self.baseFrame.bind('KeyPress', self.myquit)

        self.run_start()

    def run_start(self):
        for ball in self.balls:
            ball.move_ball()

        self.canvas.after(200, self.run_start)

    def myquit(self, e):
        self.baseFrame.destroy()


if __name__ == '__main__':
    Screen_Saver()