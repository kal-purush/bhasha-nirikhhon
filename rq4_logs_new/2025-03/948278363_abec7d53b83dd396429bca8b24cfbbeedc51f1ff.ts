import { _decorator, Component, Label, Node, Animation, Vec3, tween } from 'cc';
const { ccclass, property } = _decorator;

@ccclass('CoinView')
export class CoinView extends Component {
    @property(Animation)
    private animator: Animation = null

    @property(Label)
    private winLabel: Label = null

    private onRecycleCallBack: (coin: CoinView) => {}

    public init(cb: (coin: CoinView) => {}) {
        this.onRecycleCallBack = cb
    }

    public playStart(win: number) {
        this.winLabel.string = win.toString()
        if (this.animator)
            this.animator.play("CoinAnimationStart")
    }

    public playFadeOut(): Promise<void> {
        return new Promise((resolve) => {
            if (this.animator) {
                this.animator.play("CoinAnimationFadeOut")
                this.animator.once(Animation.EventType.FINISHED, () => {
                    resolve()
                    this.onRecycleCallBack(this)
                })
            }
        })

    }

    public playFly(pos: Vec3): Promise<void> {
        return new Promise((resolve) => {
            tween(this.node)
                .to(1, { position: pos }, { easing: "sineOut" })
                .call(async () => {
                    resolve()
                })
                .start()
        })
    }
}