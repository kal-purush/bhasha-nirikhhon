class Tool {
    static current
    static watch = GuaEvents.new()
    static rect = GuaRect.new()
    static pen = GuaPen.new()
    static clear = GuaClear.new()
    static absorb = GuaAbsorb.new() 

    //  config 相关的也应该抽成 类
    static setConfig(key, value) {
        config[key] = value
    }

    static getConfig(key) {
        return config[key]
    }

    static setPaintType(type) {
        this.current = type
        this.watchEvents(this.current)
    }

    static watchEvents(item){
        let watch = this.watch
        log('paintEvents change', item.onMouseDown)
        watch.onMouseDown = (event) => {
            item.onMouseDown(event)
        }
        watch.onMouseMove = (event) => {
            item.onMouseMove(event)
        }
        watch.onMouseUp = (event) => {
            item.onMouseUp(event)
        }
    }

    static watchKeydown(){
        bindEvent(window, "keydown",(event)=>{
            let k = event.key
            log('按下 keydown', k)
            if (k === 'c') {
                // 不知道为什么我的c键没事自动会触发，可能是因为用了什么傻逼软件了
                return
            }

            if (k === 'q') {
                this.watchEvents(this.pen)
            } else if (k === 'w') {
                this.watchEvents(this.rect)
            } else if (k === 'e') {
                this.watchEvents(this.absorb)
            } else if (k === 'f') {
                this.watchEvents(this.clear)
            } else if (k === 't') {
                let f = !this.getConfig('isFill')
                this.setConfig('isFill', f)
            } else if ('12345'.includes(k)) {
                let f = Number(k) * 3
                this.setConfig('lineWidth', f)
            }
        })
    }
}
// const guaevent = GuaEvents.new()
// const rect = GuaRect.new()
// const pen = GuaPen.new()
// const clear = GuaClear.new()
// const absorb = GuaAbsorb.new()

// const typeToggle = () => {
//     bindEvent(window, "keydown",(event)=>{
//         let k = event.key
//         log('按下 keydown', k)
//         if (k === 'c') {
//             // 不知道为什么我的c键没事自动会触发，可能是因为用了什么傻逼软件了
//             return
//         }

//         if (k === 'q') {
//             paintEvents(pen)
//         } else if (k === 'w') {
//             paintEvents(rect)
//         } else if (k === 'e') {
//             paintEvents(absorb)
//         } else if (k === 'f') {
//             paintEvents(clear)
//         } else if (k === 't') {
//             config.isFill = !config.isFill
//         } else if ('1234'.includes(k)) {
//             config.lineWidth = Number(k) * 3
//         }
//     })
// }

// const paintEvents = (item) => {
//     log('paintEvents change', item)
//     guaevent.onMouseDown = (event) => {
//         item.onMouseDown(event)
//     }
//     guaevent.onMouseMove = (event) => {
//         item.onMouseMove(event)
//     }
//     guaevent.onMouseUp = (event) => {
//         item.onMouseUp(event)
//     }
// }