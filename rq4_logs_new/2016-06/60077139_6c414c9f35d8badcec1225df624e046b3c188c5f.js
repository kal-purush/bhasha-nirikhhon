(function($) {
    "use strict";

    /**
     * Implementation of jQuery selector init.
     */
    $.fn.extend({
        ctvControl: function(options) {
            if (options && typeof (options) == 'object') {
                options = $.extend({}, $.ctvControl.defaults, options);
            }

            this.each(function() {
                new $.ctvControl(this, options);
            });
            return;
        }
    });

    $.ctvControl = function (canvas, options, arg) {
        var imagePath = typeof options.imagePath != 'undefined' ? options.imagePath : 'images';
        var DefaultConfig = {
            cardWidth: 71,
            cardHeight: 96,
            cards: [
                {"key": "Clubs-2.png", "image": "Clubs/2.png"},
                {"key": "Clubs-3.png", "image": "Clubs/3.png"},
                {"key": "Clubs-4.png", "image": "Clubs/4.png"},
                {"key": "Clubs-5.png", "image": "Clubs/5.png"},
                {"key": "Clubs-6.png", "image": "Clubs/6.png"},
                {"key": "Clubs-7.png", "image": "Clubs/7.png"},
                {"key": "Clubs-8.png", "image": "Clubs/8.png"},
                {"key": "Clubs-9.png", "image": "Clubs/9.png"},
                {"key": "Clubs-10.png", "image": "Clubs/10.png"},
                {"key": "Clubs-J.png", "image": "Clubs/J.png"},
                {"key": "Clubs-Q.png", "image": "Clubs/Q.png"},
                {"key": "Clubs-K.png", "image": "Clubs/K.png"},
                {"key": "Clubs-A.png", "image": "Clubs/A.png"},
                {"key": "Diamonds-2.png", "image": "Diamonds/2.png"},
                {"key": "Diamonds-3.png", "image": "Diamonds/3.png"},
                {"key": "Diamonds-4.png", "image": "Diamonds/4.png"},
                {"key": "Diamonds-5.png", "image": "Diamonds/5.png"},
                {"key": "Diamonds-6.png", "image": "Diamonds/6.png"},
                {"key": "Diamonds-7.png", "image": "Diamonds/7.png"},
                {"key": "Diamonds-8.png", "image": "Diamonds/8.png"},
                {"key": "Diamonds-9.png", "image": "Diamonds/9.png"},
                {"key": "Diamonds-10.png", "image": "Diamonds/10.png"},
                {"key": "Diamonds-J.png", "image": "Diamonds/J.png"},
                {"key": "Diamonds-Q.png", "image": "Diamonds/Q.png"},
                {"key": "Diamonds-K.png", "image": "Diamonds/K.png"},
                {"key": "Diamonds-A.png", "image": "Diamonds/A.png"},
                {"key": "Hearts-2.png", "image": "Hearts/2.png"},
                {"key": "Hearts-3.png", "image": "Hearts/3.png"},
                {"key": "Hearts-4.png", "image": "Hearts/4.png"},
                {"key": "Hearts-5.png", "image": "Hearts/5.png"},
                {"key": "Hearts-6.png", "image": "Hearts/6.png"},
                {"key": "Hearts-7.png", "image": "Hearts/7.png"},
                {"key": "Hearts-8.png", "image": "Hearts/8.png"},
                {"key": "Hearts-9.png", "image": "Hearts/9.png"},
                {"key": "Hearts-10.png", "image": "Hearts/10.png"},
                {"key": "Hearts-J.png", "image": "Hearts/J.png"},
                {"key": "Hearts-Q.png", "image": "Hearts/Q.png"},
                {"key": "Hearts-K.png", "image": "Hearts/K.png"},
                {"key": "Hearts-A.png", "image": "Hearts/A.png"},
                {"key": "Spades-2.png", "image": "Spades/2.png"},
                {"key": "Spades-3.png", "image": "Spades/3.png"},
                {"key": "Spades-4.png", "image": "Spades/4.png"},
                {"key": "Spades-5.png", "image": "Spades/5.png"},
                {"key": "Spades-6.png", "image": "Spades/6.png"},
                {"key": "Spades-7.png", "image": "Spades/7.png"},
                {"key": "Spades-8.png", "image": "Spades/8.png"},
                {"key": "Spades-9.png", "image": "Spades/9.png"},
                {"key": "Spades-10.png", "image": "Spades/10.png"},
                {"key": "Spades-J.png", "image": "Spades/J.png"},
                {"key": "Spades-Q.png", "image": "Spades/Q.png"},
                {"key": "Spades-K.png", "image": "Spades/K.png"},
                {"key": "Spades-A.png", "image": "Spades/A.png"}
            ],
            misc: {
                tablePattern : {"key": "table-pattern", "image": "Misc/table-pattern.png"},
                cardBackBlack : {"key": "Misc-BackBlack.png", "image": "Misc/BackBlack.png"},
                cardBackRed : {"key": "Misc-BackRed.png", "image": "Misc/BackRed.png"},
                jokerBlack : {"key": "Misc-JokerRed.png", "image": "Misc/JokerRed.png"},
                jokerRed : {"key": "Misc-JokerBlack.png", "image": "Misc/JokerBlack.png"}
            },
            tableWidth: 600,
            exportFrameWidth : 300,
            exportFrameHeight : 200,
            backgroundColor : "#0F5639"
        };
        // Linked list implementation
        function LinkedListItem(item) {
            this.item = item;
            this.next = null;
            this.previous = null;
        }

        function LinkedList() {
            this.head = null;
            this.tail = null;
            this.count = 0;
        }

        LinkedList.prototype = {

            add: function (item) {
                var listItem = new LinkedListItem(item);
                if (this.count == 0) {
                    this.head = listItem;
                    this.tail = listItem;
                } else if (this.count == 1) {
                    this.head.next = listItem;
                    this.tail = listItem;
                    listItem.previous = this.head;
                } else {
                    var node = this.tail;
                    node.next = listItem;
                    listItem.previous = node;
                    this.tail = listItem;
                }
                this.count++;
            },

            remove: function (filter) {
                var node = this.head;
                while (node != null) {
                    if (filter(node.item)) {
                        if (this.count == 1) {
                            this.tail = null;
                            this.head = null;
                        } else if (this.count == 2) {
                            if (node.previous == null) { // head item
                                node = node.next;
                                node.previous = null;
                            } else if (node.next == null) { // tail item
                                node = node.previous;
                                node.next = null;
                            }
                            this.head = node;
                            this.tail = node;
                        } else {
                            if (node.previous == null) { // head item
                                node = node.next;
                                node.previous = null;
                                this.head = node;
                            } else if (node.next == null) { // tail item
                                node = node.previous;
                                node.next = null;
                                this.tail = node;
                            } else {
                                node.previous.next = node.next;
                                node.next.previous = node.previous;
                                node.next = null;
                                node.previous = null;
                            }
                        }
                        --this.count;
                        return true;
                    }
                    node = node.next;
                }
                return false;
            },

            findFirst: function (filter) {
                var node = this.head;
                while (node != null) {
                    if (filter(node.item)) {
                        return node.item;
                    } else {
                        node = node.next;
                    }
                }

                return null;
            },

            find: function (filter) {
                var node = this.head;
                var items = [];
                while (node != null) {
                    if (filter(node.item)) {
                        items.push(node.item);
                    } else {
                        node = node.next;
                    }
                }

                return items;
            },

            moveToEnd: function (filter) {
                var item = this.findFirst(filter);

                if (item != null) {
                    this.remove(filter);
                    this.add(item);
                } else {
                    throw "Item with id[" + id + "] is not found";
                }
            },

            clear: function () {
                this.head = null;
                this.tail = null;
                this.count = 0;
            },

            getFirstItem: function () {
                return this.head;
            },

            iterate: function (applyFunction) {
                var shape = this.head;
                while (shape != null) {
                    var item = shape.item;
                    shape = shape.next;
                    if (applyFunction(item)) {
                        break;
                    }
                }
            },

            reverseIterate: function (applyFunction) {
                if (this.head == null) return;

                var shape = this.tail;

                do {
                    if (applyFunction(shape.item)) {
                        break;
                    }
                    shape = shape.previous;
                } while (shape != null);
            }
        };

        var MathUtils = {
            getDistance: function (shape1, shape2) {
                return Math.sqrt(
                    Math.pow((shape1.params.x - shape2.params.x), 2) + Math.pow((shape1.params.y - shape2.params.y), 2)
                );
            },

            numberInRange: function (value, min, max) {
                return value >= min && value <= max;
            }
        };

        // ResourceLoader responsible for loading sprite images
        function ResourceLoader() {
            this.imagesCount = 0;
            this.resources = [];
        }

        ResourceLoader.prototype = {

            addImage: function (key, imageUrl) {

                this.imagesCount++;
                var that = this;
                var image = new Image();
                image.onload = function () {
                        that.resources.push({
                            "key": key,
                            "image": image
                        });
                    image.onload = null; // cleanup
                };

                image.src = imagePath + '/' + imageUrl;
            },

            addImageWithAliases : function(keys, imageUrl) {
                this.imagesCount += keys.length;
                var that = this;
                var image = new Image();
                image.onload = function () {
                    for (var i = 0; i < keys.length; i++) {

                        that.resources.push({
                            "key": keys[i],
                            "image": image
                        });
                    }
                    image.onload = null; // cleanup
                };

                image.src = imagePath + '/' + imageUrl;
            },

            getResource: function (key) {
                for (var i = 0; i < this.resources.length; i++) {
                    if (this.resources[i].key === key) {
                        return this.resources[i].image;
                    }
                }

                return null;
            },

            clean: function () {
                this.imagesCount = 0;
                this.resources = [];
            }

        };

        /**/
        var IdGenerator = {
            id : 0,
            nextId : function() {
                return ++this.id;
            }
        }

        function ShapeObject(context, params, resourceLoader) {
            this.context = context;
            this.params = params;
            this.resourceLoader = resourceLoader;
            this.id = IdGenerator.nextId();
            this.selected = false;
        }

        ShapeObject.Type = {
            CardShape: 'cardShape',
            StaticShape: 'staticShape',
            PlayerIcon: 'playerIcon'
        };

        ShapeObject.prototype = {

            render: function () {
                this.context.save();
                this.context.translate(this.params.x, this.params.y);
                if (this.selected) {
                    this.context.beginPath();
                    this.context.rect(-(this.params.width / 2), -(this.params.height / 2), this.params.width, this.params.height);
                    this.context.lineWidth = 1;
                    this.context.strokeStyle = 'red';
                    this.context.stroke();
                }
                var image = this.resourceLoader.getResource(this.params.key);
                this.context.drawImage(image, -(this.params.width / 2), -(this.params.height / 2), this.params.width, this.params.height);
                this.context.restore();
            },

            hitTest: function (x, y) {

                return (this.params.x - this.params.width / 2 <= x && x <= (this.params.x + this.params.width / 2))
                    && (this.params.y - this.params.height / 2 <= y && y <= (this.params.y + this.params.height / 2));
            },

            update: function (x, y) {
                this.params.x = x;
                this.params.y = y;
            },
            clone : function() {

            }
        };

        function PlayerIcon(context, name, x, y, key, resourceLoader) {
            var params = {
                x: x,
                y: y,
                key: key,
                width: 50,
                height: 50,
                name : name
            };
            ShapeObject.call(this, context, params, resourceLoader);
        }

        PlayerIcon.prototype = Object.create(ShapeObject.prototype);
        PlayerIcon.prototype.constructor = PlayerIcon;

        PlayerIcon.prototype.render = function() {
            //console.log('render icon' + JSON.stringify(this.params));
            this.context.font = '15pt Calibri';
            this.context.textAlign = 'center';
            this.context.fillStyle = 'white';
            this.context.fillText(this.params.name, this.params.x, this.params.y+40);

            this.context.beginPath();
            this.context.arc(this.params.x, this.params.y, 25, 0, 2 * Math.PI, false);
            this.context.lineWidth = 3;
            this.context.strokeStyle = '#FF0000';
            this.context.stroke();

            ShapeObject.prototype.render.call(this);
        };

        function CardShape(context, x, y, key, resourceLoader) {
            var params = {
                x: x,
                y: y,
                key: key,
                width: DefaultConfig.cardWidth,
                height: DefaultConfig.cardHeight
            };
            ShapeObject.call(this, context, params, resourceLoader);
        }


        CardShape.prototype = Object.create(ShapeObject.prototype);
        CardShape.prototype.constructor = CardShape;

        function ExportFrame(canvas, context) {
            this.canvas = canvas;

            this.resize = false;
            this.resizeDirection = null;

            var params = {
                x: (DefaultConfig.tableWidth / 2),
                y: (this.canvas.height / 2),
                width: DefaultConfig.exportFrameWidth,
                height: DefaultConfig.exportFrameHeight
            };
            this.visible = false;
            this.fill = "rgba(255, 255, 255, 0.3)";

            ShapeObject.call(this, context, params);
            this.movers = [];
            // 0 1 2
            // 3   4
            // 5 6 7

            this.movers.push(new MoverShape(this.context, this.params.x - this.params.width / 2, this.params.y - this.params.height / 2));
            this.movers.push(new MoverShape(this.context, this.params.x, this.params.y - this.params.height / 2));
            this.movers.push(new MoverShape(this.context, this.params.x + this.params.width / 2, this.params.y - this.params.height / 2));
            this.movers.push(new MoverShape(this.context, this.params.x - this.params.width / 2, this.params.y));
            this.movers.push(new MoverShape(this.context, this.params.x + this.params.width / 2, this.params.y));
            this.movers.push(new MoverShape(this.context, this.params.x - this.params.width / 2, this.params.y + this.params.height / 2));
            this.movers.push(new MoverShape(this.context, this.params.x, this.params.y + this.params.height / 2));
            this.movers.push(new MoverShape(this.context, this.params.x + this.params.width / 2, this.params.y + this.params.height / 2));
        }

        ExportFrame.prototype = Object.create(ShapeObject.prototype);
        ExportFrame.prototype.constructor = ExportFrame;

        ExportFrame.prototype.update = function (x, y) {
            ShapeObject.prototype.update.call(this, x, y);
            this.movers[0].update(this.params.x - this.params.width / 2, this.params.y - this.params.height / 2);
            this.movers[1].update(this.params.x, this.params.y - this.params.height / 2);
            this.movers[2].update(this.params.x + this.params.width / 2, this.params.y - this.params.height / 2);
            this.movers[3].update(this.params.x - this.params.width / 2, this.params.y);
            this.movers[4].update(this.params.x + this.params.width / 2, this.params.y);
            this.movers[5].update(this.params.x - this.params.width / 2, this.params.y + this.params.height / 2);
            this.movers[6].update(this.params.x, this.params.y + this.params.height / 2);
            this.movers[7].update(this.params.x + this.params.width / 2, this.params.y + this.params.height / 2);
        };

        ExportFrame.prototype.checkResize = function (x, y) {
            var index = -1;
            if (this.visible) {
                for (var i = 0; i < 8; i += 1) {
                    if (this.movers[i].hitTest(x, y)) {
                        index = i;
                        break;
                    }
                }
            }
            return index;
        };

        ExportFrame.prototype.startResize = function (direction) {
            this.resizeDirection = direction;
            this.resize = true;
        };

        ExportFrame.prototype.stopResize = function () {
            this.resizeDirection = null;
            this.resize = false;
        };

        ExportFrame.prototype.handleResize = function (mx, my) {
            var diffx, diffy, newX, newY, newH, newW;

            newX = this.params.x;
            newY = this.params.y;
            newW = this.params.width;
            newH = this.params.height;

            switch (this.resizeDirection) {
                case 0:
                    diffx = ( this.params.x - this.params.width / 2 ) - mx;
                    diffy = ( this.params.y - this.params.height / 2 ) - my;
                    newX = ( this.params.x - diffx / 2 );
                    newY = this.params.y - diffy / 2;
                    newH = this.params.height + diffy;
                    newW = this.params.width + diffx;
                    break;
                case 1:
                    diffy = ( this.params.y - this.params.height / 2 ) - my;
                    newY = this.params.y - diffy / 2;
                    newH = this.params.height + diffy;
                    break;
                case 2:
                    diffx = mx - ( this.params.x + this.params.width / 2 );
                    diffy = ( this.params.y - this.params.height / 2 ) - my;
                    newX = this.params.x + diffx / 2;
                    newY = this.params.y - diffy / 2;
                    newH = this.params.height + diffy;
                    newW = this.params.width + diffx;
                    break;
                case 3:
                    diffx = ( this.params.x - this.params.width / 2 ) - mx;
                    newX = this.params.x - diffx / 2;
                    newW = this.params.width + diffx;
                    break;
                case 4:
                    diffx = mx - ( this.params.x + this.params.width / 2 );
                    newX = this.params.x + diffx / 2;
                    newW = this.params.width + diffx;
                    break;
                case 5:
                    diffx = ( this.params.x - this.params.width / 2 ) - mx;
                    diffy = my - ( this.params.y + this.params.height / 2 );

                    newX = ( this.params.x - diffx / 2 );
                    newY = this.params.y + diffy / 2;

                    newH = this.params.height + diffy;
                    newW = this.params.width + diffx;
                    break;
                case 6:
                    diffy = my - ( this.params.y + this.params.height / 2 );
                    newY = this.params.y + diffy / 2;
                    newH = this.params.height + diffy;
                    break;
                case 7:
                    diffx = mx - ( this.params.x + this.params.width / 2 );
                    diffy = my - ( this.params.y + this.params.height / 2 );

                    newX = ( this.params.x + diffx / 2 );
                    newY = this.params.y + diffy / 2;

                    newH = this.params.height + diffy;
                    newW = this.params.width + diffx;
                    break;
            }
            if ((newX + newW / 2) > DefaultConfig.tableWidth /* this.canvas.width*/ || newX - newW / 2 < 0) {
                newX = this.params.x;
                newW = this.params.width;
            }

            if ((newY + newH / 2) > (this.canvas.height) || newY - newH / 2 < 0) {
                newY = this.params.y;
                newH = this.params.height;
            }
            this.params.x = newX;
            this.params.y = newY;
            this.params.width = newW;
            this.params.height = newH;
            this.update(this.params.x, this.params.y);
        };

        ExportFrame.prototype.handleMouseOver = function (x, y) {
            var index = -1;
            for (var i = 0; i < 8; i += 1) {
                if (this.movers[i].hitTest(x, y)) {
                    index = i;
                    break;
                }
            }
            // 0  1  2
            // 3     4
            // 5  6  7

            switch (index) {
                case 0:
                    this.canvas.style.cursor = 'nw-resize';
                    break;
                case 1:
                    this.canvas.style.cursor = 'n-resize';
                    break;
                case 2:
                    this.canvas.style.cursor = 'ne-resize';
                    break;
                case 3:
                    this.canvas.style.cursor = 'w-resize';
                    break;
                case 4:
                    this.canvas.style.cursor = 'e-resize';
                    break;
                case 5:
                    this.canvas.style.cursor = 'sw-resize';
                    break;
                case 6:
                    this.canvas.style.cursor = 's-resize';
                    break;
                case 7:
                    this.canvas.style.cursor = 'se-resize';
                    break;
                case -1:
                    this.canvas.style.cursor = 'auto';
                    break;
            }
        };

        ExportFrame.prototype.render = function () {
            if (this.visible) {
                this.context.save();
                this.context.beginPath();
                this.context.rect(this.params.x - this.params.width / 2, this.params.y - this.params.height / 2, this.params.width, this.params.height);
                this.context.fillStyle = this.fill;
                this.context.fill();
                this.context.lineWidth = 1;
                this.context.strokeStyle = 'black';
                this.context.stroke();
                if (this.selected) {
                    for (var i = 0; i < this.movers.length; i++) {
                        this.movers[i].render();
                    }
                }
                this.context.restore();
            }
        };

        ExportFrame.prototype.hitTest = function (x, y) {
            if (this.visible) {
                return ShapeObject.prototype.hitTest.call(this, x, y);
            }
        };

        function MoverShape(context, x, y) {
            this.context = context;

            var params = {
                x: x,
                y: y,
                width: 4,
                height: 4
            };
            ShapeObject.call(this, context, params);
        }

        MoverShape.prototype = Object.create(ShapeObject.prototype);
        MoverShape.prototype.constructor = MoverShape;

        MoverShape.prototype.update = function (x, y) {
            this.params.x = x;
            this.params.y = y;
        };

        MoverShape.prototype.render = function () {
            this.context.beginPath();
            this.context.rect(this.params.x - this.params.width / 2, this.params.y - this.params.height / 2, this.params.width, this.params.height);
            this.context.fillStyle = this.fill;
            this.context.fill();
            this.context.lineWidth = 4;
            this.context.strokeStyle = 'black';
            this.context.stroke();
        };

        function CardsTable(canvas, context) {
            this.resourceLoader = new ResourceLoader();
            this.loadResources();
            this.shapes = new LinkedList();
            this.staticShapes = new LinkedList();
            this.canvas = canvas;
            this.context = context;
            this.dnd = {dragging: false};
            this.showTable = false;
            this.exportFrame = new ExportFrame(canvas, context);
            this.background = DefaultConfig.backgroundColor;
        }

        CardsTable.prototype = {
            init: function () {
                this.populateStaticShapes();
                this.background = 'pattern';
            },

            isLoadingCompleted: function () {
                return this.resourceLoader.imagesCount === this.resourceLoader.resources.length;
            },

            loadResources: function () {
                for (var i = 0; i < DefaultConfig.cards.length; i++) {
                    this.resourceLoader.addImage(DefaultConfig.cards[i].key, DefaultConfig.cards[i].image);
                }
                this.resourceLoader.addImage(DefaultConfig.misc.tablePattern.key, DefaultConfig.misc.tablePattern.image);
                this.resourceLoader.addImage(DefaultConfig.misc.cardBackBlack.key, DefaultConfig.misc.cardBackBlack.image);
                this.resourceLoader.addImage(DefaultConfig.misc.cardBackRed.key, DefaultConfig.misc.cardBackRed.image);
                this.resourceLoader.addImage(DefaultConfig.misc.jokerBlack.key, DefaultConfig.misc.jokerBlack.image);
                this.resourceLoader.addImage(DefaultConfig.misc.jokerRed.key, DefaultConfig.misc.jokerRed.image);
            },

            render: function () {
                this.drawTable();
                this.drawShapes();
            },

            renderSplash : function() {
                this.drawTable();
            },

            createShape: function (shapeType, key, coords) {
                var object = null;
                switch (shapeType) {
                    case ShapeObject.Type.CardShape:
                        object = new CardShape(this.context, coords.x, coords.y, key, this.resourceLoader);
                        this.shapes.add(object);
                        break;
                    case ShapeObject.Type.StaticShape:
                        object = new CardShape(this.context, coords.x, coords.y, key, this.resourceLoader);
                        object.params.visible = true;
                        this.staticShapes.add(object);
                        break;
                    case ShapeObject.Type.PlayerIcon:
                        console.log('Create player icon');
                        object = new PlayerIcon(this.context, coords.name, coords.x, coords.y, key, this.resourceLoader);
                        object.params.visible = true;
                        this.staticShapes.add(object);
                        break;
                }
            },

            drawShapes: function () {

                this.staticShapes.iterate(function (staticShape) {
                    if (staticShape.params.visible)
                        staticShape.render();
                });
                this.shapes.iterate(function (item) {
                    item.render();
                });

                this.exportFrame.render();
            },

            drawTable: function () {
                this.context.save();

                if (this.background == 'pattern') {
                    this.context.fillRect(0, 0, this.canvas.width, this.canvas.height);
                    var pattern = this.context.createPattern(this.resourceLoader.getResource(DefaultConfig.misc.tablePattern.key), 'repeat');
                    this.context.clearRect(0, 0, this.canvas.width, this.canvas.height);

                    this.context.fillStyle = pattern;
                    this.context.fillRect(0, 0, this.canvas.width, this.canvas.height); // context.fillRect(x, y, width, height);
                } else {
                    this.context.fillStyle = this.background;
                    this.context.fillRect(0, 0, this.canvas.width, this.canvas.height);
                }
                this.context.restore();

                this.context.save();

                this.context.beginPath();
                context.moveTo(DefaultConfig.tableWidth, 0);
                context.lineTo(DefaultConfig.tableWidth, this.canvas.height);
                this.context.lineWidth = 2;
                this.context.setLineDash([10,5]);
                this.context.strokeStyle = '#000000';
                this.context.stroke();
                this.context.restore();
            },

            populateStaticShapes: function () {

                var padding = 2;
                var border = 2;
                var posY = DefaultConfig.cardHeight / 2 + border*2;
                var posX = DefaultConfig.tableWidth + border*2 + DefaultConfig.cardWidth / 2;
                var additionalShapes = [
                    DefaultConfig.misc.cardBackBlack,
                    DefaultConfig.misc.cardBackRed,
                    DefaultConfig.misc.jokerBlack,
                    DefaultConfig.misc.jokerRed
                ];
                for (var i = 0; i < 4; i++) {
                    for (var j = 0; j < 13; j++) {
                        var index = j + i * 13;
                        var card = DefaultConfig.cards[index];
                        this.createShape(ShapeObject.Type.StaticShape, card.key, {x: posX, y: posY});
                        posY += DefaultConfig.cardWidth / 2.5 + padding;//(DefaultConfig.cardWidth + padding);
                    }

                    this.createShape(ShapeObject.Type.StaticShape, additionalShapes[i].key, {x: posX, y: posY});
                    posY = DefaultConfig.cardHeight / 2 + border*2;//border + DefaultConfig.cardWidth / 2;
                    posX += DefaultConfig.cardWidth + border;
                }
            },

            exportTable: function () {
                var that = this;
                var exportJson = {
                    cards: [],
                    frame: false,
                    table: false,
                    background : that.background
                };

                this.shapes.iterate(function (item) {
                    exportJson.cards.push({x: item.params.x, y: item.params.y, key: item.params.key});
                });

                exportJson.frame = this.exportFrame.visible;
                exportJson.frameParams = this.exportFrame.params;

                if (this.showTable) {
                    exportJson.table = true;
                }

                return exportJson;
            },

            loadTable: function (conf) {
                this.staticShapes.iterate(function (staticItem) {
                    staticItem.params.visible = true;
                });
                this.shapes.clear();

                this.showTable = conf.table;
                if (conf.frame) {
                    this.exportFrame.visible = true;
                    this.exportFrame.params.width = conf.frameParams.width;
                    this.exportFrame.params.height = conf.frameParams.height;
                    this.exportFrame.update(conf.frameParams.x, conf.frameParams.y);
                }
                for (var i = 0; i < conf.cards.length; i++) {
                    var card = conf.cards[i];
                    this.createShape(ShapeObject.Type.CardShape, card.key, {x: card.x, y: card.y});
                    if (card.key != DefaultConfig.misc.cardBackBlack.key && card.key != DefaultConfig.misc.cardBackRed.key) {
                        this.staticShapes.iterate(function (staticShape) {
                            if (staticShape.params.key === card.key) {
                                staticShape.params.visible = false;
                                return true;
                            }
                        });
                    }
                }
                this.render();
            },

            isMouseOnTable: function (mouse) {
                var tableArea = {
                    x: 0,
                    y: 0,
                    width: DefaultConfig.tableWidth,
                    height: this.canvas.height
                };
                return (MathUtils.numberInRange(mouse.x, tableArea.x, tableArea.width)
                && MathUtils.numberInRange(mouse.y, tableArea.y, tableArea.height));
            },

            isShapeOnTable: function (shape) {

                var tableArea = {
                    x: 0,
                    y: 0,
                    width: DefaultConfig.tableWidth,
                    height: this.canvas.height
                };
                var minOverlap = (MathUtils.numberInRange(shape.params.x - shape.params.width / 2, tableArea.x, tableArea.width)
                && MathUtils.numberInRange(shape.params.y - shape.params.y / 2, tableArea.y, tableArea.height));

                var maxOverlap = (MathUtils.numberInRange(shape.params.x + shape.params.width / 2, tableArea.x, tableArea.width)
                && MathUtils.numberInRange(shape.params.y + shape.params.height / 2, tableArea.y, tableArea.height));

                return minOverlap && maxOverlap;
            },
            // shape2 is static and shape1 should be moved to position of shape2
            animate: function (shape1, shape2, finishCallback) {
                var that = this;
                var speed = 10;
                var angle = Math.atan2(shape1.params.y - shape2.params.y, shape1.params.x - shape2.params.x);
                var distance = MathUtils.getDistance(shape1, shape2);
                var len = distance / speed;
                shape1.params.selectable = false;

                (function worker(shape1, shape2, angle, len, currLen) {
                    if (currLen < len) {

                        shape1.params.x -= speed * Math.cos(angle);
                        shape1.params.y -= speed * Math.sin(angle);

                        that.render();
                        if (MathUtils.numberInRange(Math.abs(shape1.params.x) - Math.abs(shape2.params.x), 0, 1)
                            && MathUtils.numberInRange((Math.abs(shape1.params.y) - Math.abs(shape2.params.y)), 0, 1)) {

                            shape1.params.x = shape2.params.x;
                            shape1.params.y = shape2.params.y;
                            finishCallback();
                            return;// finish function
                        }

                        setTimeout(function () {
                            worker(shape1, shape2, angle, len, ++currLen);
                        }, 1);

                    } else {
                        finishCallback();
                        return; // done
                    }
                })(shape1, shape2, angle, len, 0);
            },

            handleMouseDown: function (mouse) {
                this.shapes.iterate(function (item) {
                    item.selected = false;
                });

                var direction = this.exportFrame.checkResize(mouse.x, mouse.y);
                if (direction != -1) {
                    this.exportFrame.startResize(direction);
                } else if (this.exportFrame.hitTest(mouse.x, mouse.y)) {
                    this.dnd = {
                        x: mouse.x - this.exportFrame.params.x,
                        y: mouse.y - this.exportFrame.params.y,
                        dragging: true,
                        draggingItem: this.exportFrame
                    };

                    this.exportFrame.selected = true;
                } else {
                    var that = this;

                    this.exportFrame.selected = false;
                    if (!this.isMouseOnTable(mouse)) { // if mouse clicked out of gambling table

                        this.staticShapes.reverseIterate(function (staticItem) {
                            if (staticItem.hitTest(mouse.x, mouse.y) && staticItem.params.visible) {
                                that.createShape(ShapeObject.Type.CardShape, staticItem.params.key, staticItem.params);

                                if (staticItem.params.key != DefaultConfig.misc.cardBackRed.key
                                    && staticItem.params.key != DefaultConfig.misc.cardBackBlack.key) {
                                    staticItem.params.visible = false;
                                }

                                return true;
                            }
                        });
                    }

                    this.shapes.reverseIterate(function (item) {
                        if (item.hitTest(mouse.x, mouse.y)) {
                            that.dnd = {
                                x: mouse.x - item.params.x,
                                y: mouse.y - item.params.y,
                                dragging: true,
                                draggingItem: item
                            };

                            item.selected = true;
                            that.shapes.moveToEnd(function (filterItem) {
                                if (item.id == filterItem.id)
                                    return true;
                            });
                            return true;
                        }
                    });
                }
                this.render();
            },

            handleMouseMove: function (mouse) {
                if (this.exportFrame.resize) {
                    this.exportFrame.handleResize(mouse.x, mouse.y);
                    this.render();
                } else if (table.dnd.dragging) {
                    var posX, posY;

                    var minX = this.dnd.draggingItem.params.width / 2;
                    var maxX = this.canvas.width - this.dnd.draggingItem.params.width / 2;

                    var minX, maxX, minY, maxY;
                    if (this.dnd.draggingItem === this.exportFrame) {
                        minX = this.dnd.draggingItem.params.width / 2;
                        maxX = DefaultConfig.tableWidth - this.dnd.draggingItem.params.width / 2;

                        minY = this.dnd.draggingItem.params.height / 2;
                        maxY = this.canvas.height - ( this.dnd.draggingItem.params.height / 2 );
                    } else {
                        minX = this.dnd.draggingItem.params.width / 2;
                        maxX = this.canvas.width - this.dnd.draggingItem.params.width / 2;

                        minY = this.dnd.draggingItem.params.height / 2;
                        maxY = this.canvas.height - this.dnd.draggingItem.params.height / 2;
                    }

                    //clamp x and y positions to prevent object from dragging outside of canvas
                    posX = mouse.x - this.dnd.x;
                    posX = (posX < minX) ? minX : ((posX > maxX) ? maxX : posX);
                    posY = mouse.y - this.dnd.y;
                    posY = (posY < minY) ? minY : ((posY > maxY) ? maxY : posY);

                    this.dnd.draggingItem.update(posX, posY);
                    this.render();
                } else if (table.exportFrame.selected) {
                    this.exportFrame.handleMouseOver(mouse.x, mouse.y);
                }
            },

            handleMouseUp: function (mouse) {
                if (this.exportFrame.resize) {
                    this.exportFrame.stopResize();
                } else if (this.dnd.dragging) {
                    var shape = this.dnd.draggingItem;

                    if (this.exportFrame !== shape) {
                        if (!this.isShapeOnTable(shape)) {
                            var that = this;
                            var staticShape = this.staticShapes.findFirst(function (item) {
                                if (item.params.key == shape.params.key) {
                                    return true;
                                }
                            });
                            var callback = function () {
                                staticShape.params.visible = true;
                                that.shapes.remove(function (filterItem) {
                                    if (shape.id == filterItem.id) return true;
                                });
                                that.render();
                            };
                            this.animate(shape, staticShape, callback);
                        }
                    }
                    this.dnd = {dragging: false};
                }
            },
            api: function () {
                var that = this;
                return {
                    toggleTable: function () {
                        that.showTable = !that.showTable;
                        that.render();
                    },

                    toggleFrame: function () {
                        that.exportFrame.visible = !that.exportFrame.visible;
                        that.render();
                    },

                    clear: function () {
                        that.staticShapes.iterate(function (staticItem) {
                            staticItem.params.visible = true;
                        });
                        that.shapes.clear();
                        that.render();
                    },

                    getConfiguration: function () {
                        return that.exportTable();
                    },

                    loadConfiguration: function (configuration) {
                        that.loadTable(configuration);
                    },

                    setBackground : function(color) {
                        that.background = color;
                        that.render();
                    }
                };
            }
        };

        function getEventXY(evt) {
            //getting mouse position correctly, being mindful of resizing that may have occur in the browser:
            var rect = canvas.getBoundingClientRect();
            var mouseX = (evt.clientX - rect.left) * (canvas.width / rect.width);
            var mouseY = (evt.clientY - rect.top) * (canvas.height / rect.height);
            return {x: mouseX, y: mouseY};
        }

        function mouseDownListener(evt) {
            canvas.removeEventListener("mousedown", mouseDownListener, false);
            canvas.addEventListener("mouseup", mouseUpListener, false);

            var mouse = getEventXY(evt);
            table.handleMouseDown(mouse);

            //code below prevents the mouse down from having an effect on the main browser window:
            if (evt.preventDefault) {
                evt.preventDefault();
            } //standard
            else if (evt.returnValue) {
                evt.returnValue = false;
            } //older IE
            return false;
        };

        function mouseMoveListener(evt) {
            var mouse = getEventXY(evt);
            table.handleMouseMove(mouse);
        };

        function mouseUpListener(evt) {
            canvas.addEventListener("mousedown", mouseDownListener, false);
            canvas.removeEventListener("mouseup", mouseUpListener, false);

            var mouse = getEventXY(evt);
            table.handleMouseUp(mouse);
        };

        /*  Local Scope variables, should be available in mouse listeners */
        var context = canvas.getContext("2d");
        var table = new CardsTable(canvas, context);

        (function start() {
            if (table.isLoadingCompleted()) {
                table.init();
                table.render();
                canvas.addEventListener("mousedown", mouseDownListener, false);
                document.addEventListener("mousemove", mouseMoveListener, false);
                options.initExternalListeners(table.api());
            } else {
                console.log('loading')
                table.renderSplash();
                setTimeout(start, 500);
            }
        })();
    };
})(jQuery);