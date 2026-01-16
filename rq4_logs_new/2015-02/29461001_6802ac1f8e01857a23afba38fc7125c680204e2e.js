/**
 * @file 媒体播放器
 * @author musicode
 */
define(function (require, exports, module) {

    'use strict';

    /**
     * @constructor
     * @param {Object} options
     * @property {jQuery} options.element
     * @property {string} options.src
     */
    function MediaPlayer(options) {
        $.extend(this, MediaPlayer.defaultOptions, options);
        this.init();
    }

    MediaPlayer.prototype = {

        constructor: MediaPlayer,

        /**
         * 初始化播放器
         */
        init: function () {

            var me = this;



            me.setActiveVideo(0);

        },

        /**
         * 绑定播放器界面相关的交互事件
         */
        initPlayer: function () {

            var me = this;

            if (me.poster) {
                me.setPoster(me.poster);
            }

            var element = me.element;

            var quality = me.quality;

            var progressBar = element.find(selector.PROGRESS_BAR);
            var seekHandle = element.find(selector.SEEK_HANDLE);

            var volumeBar = element.find(selector.VOLUME_BAR);
            var volumeHandle = element.find(selector.VOLUME_HANDLE);

            var playVideo = function () {
                me.play();
            };

            var pos2Time = function (pos) {

                var progressBarWidth = progressBar.innerWidth();
                var seekHandleWidth = seekHandle.outerWidth(true);

                var max = progressBarWidth - seekHandleWidth;

                if (pos > max) {
                    pos = max;
                }

                me.setCurrentTime(
                    (pos / max) * me.getDuration()
                );
            };

            var pos2Volume = function (pos) {

                var volumeBarHeight = volumeBar.innerHeight();
                var volumeHandleHeight = volumeHandle.outerHeight(true);

                var max = volumeBarHeight - volumeHandleHeight;

                if (pos > max) {
                    pos = max;
                }

                pos = max - pos;

                me.setVolume(
                    pos / max
                );

            };

            var clickType = supportEvent.click;

            // 在大块面积上慎用 touch，因为一碰就会播放，无法平移操作
            element
            .on('click', 'video', function () {
                if (me.isPaused()) {
                    me.play();
                }
                else {
                    me.pause();
                }
            })
            .on('click', selector.POSTER, playVideo)
            .on(clickType, '.' + selector.CLASS_PLAY, playVideo)
            .on(clickType, '.' + selector.CLASS_PAUSE, function () {
                me.pause();
            })
/**
            .on(clickType, '.' + selector.CLASS_MUTED, function () {
                me.setMute(false);
            })
            .on(clickType, '.' + selector.CLASS_UNMUTED, function () {
                me.setMute(true);
            })
*/
            .on(clickType, '.' + selector.CLASS_EXPAND, function () {
                fullScreen.enter();
            })
            .on(clickType, '.' + selector.CLASS_COMPRESS, function () {
                fullScreen.exit();
            })
            .on(clickType, selector.PROGRESS_PANEL, function (e) {
                var progressPanel = element.find(selector.PROGRESS_PANEL);
                var x = supportEvent.pageX(e) - progressPanel.offset().left;
                pos2Time(x);
            })
            .on(clickType, selector.VOLUME_BAR, function (e) {
                var y = supportEvent.pageY(e) - volumeBar.offset().top;
                pos2Volume(y);
            })
            .on(clickType, selector.QUALITY_LOW, function () {
                me.setQuality('low');
            })
            .on(clickType, selector.QUALITY_HIGH, function () {
                me.setQuality('high');
            })
            .on(clickType, selector.QUALITY_SUPER, function () {
                me.setQuality('super');
            });

            fullScreen.change(function (isFullScreen) {

                if (isFullScreen) {
                    me.enterFullScreen();
                }
                else {
                    me.exitFullScreen();
                }

            });

            if (quality) {

                me.qualityPopup = new Popup({
                    element: element.find(selector.QUALITY),
                    layer: element.find(selector.QUALITY_PANEL),
                    show: {
                        trigger: clickType
                    },
                    hide: {
                        trigger: clickType
                    }
                });
            }

            new Popup({
                element: element.find(selector.MUTE),
                layer: element.find(selector.VOLUME_PANEL),
                show: {
                    trigger: clickType
                },
                hide: {
                    trigger: clickType
                }
            });

            new Draggable({
                element: seekHandle,
                container: progressBar,
                axis: 'x',
                silence: true,
                onDrag: function (e, data) {
                    pos2Time(data.left);
                }
            });

            new Draggable({
                element: volumeHandle,
                container: volumeBar,
                axis: 'y',
                silence: true,
                onDrag: function (e, data) {
                    pos2Volume(data.top);
                }
            });

        },

        /**
         * 设置画质
         *
         * @param {string} type 可选值包括 low high super
         */
        setQuality: function (type) {

            var me = this;
            var quality = me.quality;

            if (!quality || !quality[ type ]) {
                throw new Error('[setQuality] 找不到 ' + type + ' 画质.');
            }

            var data = quality[ type ];

            me.setProperty('src', data.url);

            me.element
                .find(selector.CURRENT_QUALITY)
                .text(data.text);

            var qualityPopup = me.qualityPopup;

            var qualityPanel = qualityPopup.layer;
            var activeQualityClass = selector.CLASS_QUALITY_ACTIVE;

            qualityPanel
                .find('.' + activeQualityClass)
                .removeClass(activeQualityClass);

            qualityPanel
                .find(qualitySelector[ type ])
                .addClass(activeQualityClass);

            if (!qualityPopup.hidden) {
                qualityPopup.close();
            }

        },

        /**
         * 设置当前激活的视频
         *
         * @param {number} index
         */
        setActiveVideo: function (index) {

            var me = this;

            var video = me.video;

            video.off(videoNamespace);

            var mainProperty = me.mainProperty;
            var property = $.extend({ }, me.sharedProperty);

            if (index === me.mainIndex) {
                $.extend(property, mainProperty);
            }
            else {
                property.src = me.list[index];
                if (index === 0) {
                    property.poster = mainProperty.poster;
                }
            }

            video.prop(
                property
            );

            listen(me, video);

            me.index = index;

        },

        /**
         * 获取封面地址
         *
         * @return {string}
         */
        getPoster: function () {
            return this.poster || '';
        },

        /**
         * 设置封面地址
         *
         * @param {string} poster
         */
        setPoster: function (poster) {

            var me = this;

            me.poster = poster;

            var posterElement = me.element.find(selector.POSTER);

            posterElement.css(
                'background-image',
                'url(' + poster + ')'
            );

        },

        /**
         * 播放视频
         */
        play: function () {

            var me = this;

            if (me.index === me.list.length) {
                me.setActiveVideo(me.mainIndex);
            }

            me.video[0].play();
        },

        /**
         * 暂停视频
         */
        pause: function () {
            this.video[0].pause();
        },

        /**
         * 是否暂停播放
         *
         * @return {boolean}
         */
        isPaused: function () {
            return this.video.prop('paused');
        },

        /**
         * 更新加载进度条
         *
         * @param {number} time 已加载的时间
         */
        updateLoadProgress: function (time) {

            var me = this;
            var loadProgress = me.element.find(selector.LOAD_PROGRESS);

            loadProgress.width(
                lib.percent(
                    time,
                    me.getDuration()
                )
            );
        },

        /**
         * 获取音量
         *
         * @return {number}
         */
        getVolume: function () {
            return this.getProperty('volume');
        },

        /**
         * 设置音量
         *
         * @param {number} volume 音量值，从 0 到 1，0 表示静音
         */
        setVolume: function (volume) {

            var me = this;

            if (volume >= 0 && volume <= 1) {

                me.setProperty('volume', volume);

                me.updateVolume(volume);

            }
        },

        /**
         * 更新音量的 DOM
         *
         * @param {number} volume 音量，0 - 1
         */
        updateVolume: function (volume) {

            var element = this.element;

            var volumeBar = element.find(selector.VOLUME_BAR);
            var volumeProgress = volumeBar.find(selector.VOLUME_PROGRESS);
            var volumeHandle = element.find(selector.VOLUME_HANDLE);

            var volumeBarHeight = volumeBar.innerHeight();
            var volumeHandleHeight = volumeHandle.outerHeight(true);

            var bottom = volume * volumeBarHeight;

            volumeProgress.height(bottom);

            bottom -= volumeHandleHeight;

            if (bottom < 0) {
                bottom = 0;
            }
            else if (bottom + volumeHandleHeight > volumeBarHeight) {
                bottom = volumeBarHeight - volumeHandleHeight;
            }

            volumeHandle.css({
                top: 'auto',
                bottom: bottom
            });

        },

        /**
         * 是否静音
         *
         * @return {boolean}
         */
        isMuted: function () {
            return this.getProperty('muted');
        },

        /**
         * 设置是否为静音
         *
         * @param {boolean} muted
         */
        setMute: function (muted) {

            var me = this;

            me.setProperty('muted', muted);

            me.updateVolume(
                muted ? 0 : me.getVolume()
            );

        },

        /**
         * 获取视频的长度，以秒为单位
         *
         * @return {number}
         */
        getDuration: function () {
            return this.getProperty('duration');
        },

        /**
         * 更新总时长的 DOM
         *
         * @param {number} time 总时长，单位为秒
         */
        updateDuration: function (time) {

            var me = this;

            var target = me.element.find(selector.DURATION);

            target.text(
                me.formatTime(time)
            );
        },

        /**
         * 获取视频当前播放位置
         *
         * @return {number}  播放位置，单位为秒
         */
        getCurrentTime: function () {
            return this.getProperty('currentTime');
        },

        /**
         * 设置视频当前播放位置
         *
         * @param {number} time 播放位置，单位为秒
         */
        setCurrentTime: function (time) {

            var me = this;

            me.setProperty('currentTime', time);
            me.updateCurrentTime(time);
        },

        /**
         * 更新当前播放时间的 DOM
         *
         * @param {number} time 播放位置，单位为秒
         */
        updateCurrentTime: function (time) {

            var me = this;
            var element = me.element;

            var currentTime = element.find(selector.CURRENT_TIME);
            currentTime.text(
                me.formatTime(time)
            );

            var duration = me.getDuration();

            var playProgress = element.find(selector.PLAY_PROGRESS);
            playProgress.width(
                lib.percent(time, duration)
            );

            var progressBar = element.find(selector.PROGRESS_BAR);
            var seekHandle = element.find(selector.SEEK_HANDLE);
            var progressBarWidth = progressBar.innerWidth();
            var seekHandleWidth = seekHandle.outerWidth(true);

            var left = (time / duration) * progressBarWidth;

            left -= seekHandleWidth;

            if (left < 0) {
                left = 0;
            }
            else if (progressBarWidth - left < seekHandleWidth) {
                left = progressBarWidth - seekHandleWidth;
            }

            seekHandle.css('left', left);

        },

        /**
         * 获取正片的属性
         *
         * @return {*}
         */
        getProperty: function (name) {
            var me = this;
            var value = me.mainProperty[ name ];
            if (value == null) {
                value = me.video.prop(name);
            }
            return value;
        },

        /**
         * 设置正片的属性
         *
         * @param {string} name
         * @param {*} value
         */
        setProperty: function (name, value) {

            var me = this;

            var isShared = sharedProperties.indexOf(name) > -1;

            var target = isShared ? 'sharedProperty' : 'mainProperty';

            me[ target ][ name ] = value;

            if (isShared || me.index === me.mainIndex) {
                me.video.prop(name, value);
            }

        },

        /**
         * 格式化时间，显示格式为 00:00
         *
         * @param {number} time 时间，单位为秒
         * @return {string}
         */
        formatTime: function (time) {
            return lib.formatTime(
                time,
                this.getDuration() / lib.TIME_HOUR >= 1
            );
        },

        /**
         * 销毁对象
         */
        dispose: function () {

            var me = this;

            $.each(
                me,
                function (key) {
                    var target = me[key];
                    if (target.jquery) {
                        target.off();
                        me[key] = null;
                    }
                }
            );
        }

    };

    return MediaPlayer;

});