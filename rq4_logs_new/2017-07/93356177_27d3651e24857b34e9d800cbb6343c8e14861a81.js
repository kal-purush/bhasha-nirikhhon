(function (exports) {

    // 八个方向的方向指示器数组
    var orientationsGroup = [
        [[1, 0], [-1, 0]],
        [[0, 1], [0, -1]],
        [[1, 1], [-1, -1]],
        [[-1, 1], [1, -1]]
    ];
    function Board(size) {
        this.map = new Array(size)
        this.size = size
        for (var i = 0; i < size; i++) {
            this.map[i] = new Array(size)
            this.map[i].fill(null)
        }
    }

    // 清空棋盘
    Board.prototype.clear = function () {
        Board.call(this, this.size)
    }

    // 在某一点落子
    Board.prototype.stepOn = function (piece) {
        var x = piece.x, y = piece.y
        if (!this.map[x][y]) {
            this.map[x][y] = piece
            return this.map[x][y]
        }
        return null
    }

    // 取消某一点的落子(悔棋)
    Board.prototype.stepOff = function (piece) {
        this.map[piece.x][piece.y] = null
        return
    }

    // 判断是否赢棋，落子时判断
    Board.prototype.checkWin = function (piece) {
        var x = piece.x, y = piece.y,
            originX = x, originY = y,
            map = this.map, size = this.size
            isWin = false, that = this

        orientationsGroup.forEach(function (orientations) {
            // 计算横竖交叉四个方向的最大长度
            var maxLength = 0,
            nextPositions = [],
            detectWinRange = []

            if (!isWin) {
                orientations.forEach(function (orientation, direction) {
                    var x = originX, y = originY, index = 0,
                        xOri = orientation[0], yOri = orientation[1]
                    // 先把重复的位置删除
                    detectWinRange.pop()
                    while (x >= 0 && x < size && y >= 0 && y < size && index++ < 5) {
                        if (map[x] && map[x][y] && map[x][y].isBlack === piece.isBlack) {
                            maxLength++
                        }
                        // 以piece点为中心，把周围直线上最多10个位置加入数组
                        if (direction === 0) {
                            detectWinRange.unshift({ x: x, y: y })
                        } else {
                            detectWinRange.push({ x: x, y: y })
                        }
                        x = x + xOri
                        y = y + yOri
                    }
                })

                // 重复计算了一次，所以要-1
                maxLength--
                if (maxLength === 5) {
                    // 任意一个方向满了5个，赢了
                    isWin = true
                }
                that.addWinIndications(piece, detectWinRange)
            }
        })
        return isWin
    }

    // 添加赢棋提示
    Board.prototype.addWinIndications = function (piece, detectWinRange) {
        var size = detectWinRange.length - 4,
            map = this.map, matchedCount = 0, freePosition = null
        for (var i = 0; i < size ; i++) {
            matchedCount = 0
            freePosition = null
            detectWinRange.slice(i, i + 5).forEach(function (range) {
                var item = map[range.x] && map[range.x][range.y]
                if (item && item.isBlack === piece.isBlack) { // 为匹配的棋子
                    matchedCount++
                } else if (item === null) { // 空位
                    freePosition = { x: range.x, y: range.y }
                }
            })
            if (matchedCount === 4 && freePosition) { // 刚好有4个匹配棋子和一个空位的时候，就是赢棋了！
                piece.winIndications.push(freePosition)
            }
        }
    }

    exports.Board = Board
})(window)