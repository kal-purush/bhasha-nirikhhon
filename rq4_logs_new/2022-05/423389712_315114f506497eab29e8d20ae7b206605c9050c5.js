; (
    function () {
        //put it into global scope
        window.Clock = Clock;

        function Clock(ctx) {
            this.context = ctx;
            this.canvas = ctx.canvas;
            this.xPosition = this.canvas.width / 2;
            this.yPosition = this.canvas.height / 2;

        }

        function getTime() {
            var date = new Date();
            return {
                hour: date.getHours() - 12,
                minute: date.getMinutes(),
                seconds: date.getSeconds(),
                millSeconds: date.getMilliseconds()
            }
        }

        //clock init
        Clock.prototype.init = function () {
            console.log('init');
            setInterval(
                () =>
                    window.requestAnimationFrame(this.draw.bind(this)),
                1 / 60);
        }

        //clock draw
        Clock.prototype.draw = function () {
            this.date = getTime();
            this.context.save();
            this.drawPanel();
            this.drawNumber();
            this.drawHourPoint(this.date.hour, this.date.minute);
            this.drawMinutePoint(this.date.minute);
            this.drawSecondsPoint(this.date.seconds)
            this.drawCirlce();
            this.context.restore();
        }

        //draw clock panel
        Clock.prototype.drawPanel = function () {
            var ctx = this.context;
            var width = this.canvas.width / 2;
            var height = this.canvas.height / 2;
            var space = 40;

            ctx.save();

            // ctx.strokeStyle = 'yellow';
            ctx.beginPath();
            ctx.fillStyle = 'white'
            ctx.arc(width, height, width - 30, 0, 2 * Math.PI, false);
            ctx.fill();

            ctx.restore();

        }

        //draw text midea
        Clock.prototype.drawText = function () {
            var ctx = this.context;
            var xPosition = this.canvas.width / 2;
            var yPosition = this.canvas.height / 2;
            ctx.save();

            ctx.fillStyle = 'yellow'
            ctx.font = 'italic 48px serif ';
            ctx.textAlign = 'center';
            ctx.fillText('Midea', xPosition, yPosition);

            ctx.font = 'italic 24px serif ';

            // ctx.translate(xPosition, yPosition);
            ctx.beginPath();
            // ctx.moveTo(xPosition, yPosition);
            ctx.strokeStyle = 'yellow';
            ctx.lineWidth = 4;
            ctx.arc(xPosition - 72, yPosition - 30, 40, Math.PI / 12 * 2, 2 * Math.PI * 0.95, false);
            ctx.stroke();


            ctx.restore()
        }

        //fill the num of clock
        Clock.prototype.drawNumber = function () {
            var ctx = this.context;
            var numArray = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
            var r = this.canvas.width / 2 - 70;

            ctx.save();
            ctx.beginPath();
            ctx.font = '40px Arial';
            // ctx.fillStyle = 'yellow';
            ctx.lineWidth = 10;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.translate(this.xPosition, this.yPosition);

            numArray.forEach(function (item, index) {
                var angle = Math.PI / 6;
                var x = r * Math.cos(angle * (index - 2));
                var y = r * Math.sin(angle * (index - 2));
                ctx.fillText(item, x, y);
            })
            ctx.restore();
        }

        //fill circle
        Clock.prototype.drawCirlce = function () {
            var ctx = this.context;

            ctx.save();

            ctx.fillStyle = '#424242'
            // 如果不加这个东西，画圆的半径会失效.
            // 作用是把之前的笔给注销掉，否则下面的操作会重复之前的笔触
            ctx.beginPath();
            // ctx.moveTo(this.xPosition, this.yPosition);
            ctx.lineWidth = 20;
            ctx.arc(this.xPosition, this.yPosition, 13, 0, Math.PI * 2, false);
            ctx.fill();

            ctx.beginPath();
            ctx.fillStyle = '#99999999'
            ctx.arc(this.xPosition, this.yPosition, 6, 0, Math.PI * 2, false)
            ctx.fill();

            ctx.restore();
        }

        //fill hour point
        Clock.prototype.drawHourPoint = function (hour, minute) {
            var ctx = this.context;

            ctx.save();

            ctx.translate(this.xPosition, this.yPosition);
            //太小了，就看不清楚了
            ctx.lineWidth = 10;
            ctx.lineCap = 'round';
            ctx.fillStyle = '#424242'
            //清除之前的笔的，或者是路径的设定
            // ctx.beginPath();
            // 这个作用是移动笔触到指定的位置
            ctx.moveTo(0, 0);

            var hourAngle = Math.PI / 6 * hour;
            var minAngle = Math.PI / 6 / 60 * minute;
            ctx.rotate(hourAngle + minAngle);
            ctx.lineTo(0, this.xPosition * 0.45 * -1);
            ctx.stroke();



            ctx.restore()
        }

        // fill minute point
        Clock.prototype.drawMinutePoint = function (minute) {
            var ctx = this.context;

            ctx.save();
            ctx.beginPath();

            ctx.translate(this.xPosition, this.yPosition);
            //太小了，就看不清楚了
            ctx.lineWidth = 10;
            ctx.lineCap = 'round';
            ctx.strokeStyle = '#424242'
            //清除之前的笔的，或者是路径的设定
            // 这个作用是移动笔触到指定的位置
            ctx.moveTo(0, 0);

            var minAngle = Math.PI / 30 * minute;
            ctx.rotate(minAngle);
            ctx.lineTo(0, this.xPosition * 0.55 * -1);
            ctx.stroke();

            ctx.restore();
        }

        // fill Seconds point
        Clock.prototype.drawSecondsPoint = function (seconds) {
            var ctx = this.context;
            var millSeconds = this.date.millSeconds;
            // console.log(this.date.millSeconds);
            // console.log(this.date.seconds);
            ctx.save();
            ctx.beginPath();

            ctx.translate(this.xPosition, this.yPosition);
            //太小了，就看不清楚了
            ctx.lineWidth = 10;
            ctx.lineCap = 'round';
            ctx.strokeStyle = '#e08946'
            ctx.moveTo(0, 0);

            var minAngle = Math.PI / 30 * seconds;
            var millMinAngle = Math.PI / 30 / 1000 * millSeconds
            ctx.rotate(minAngle + millMinAngle);
            ctx.lineTo(0, this.xPosition * 0.60 * -1);
            ctx.stroke();

            ctx.restore();
        }
    }
)()