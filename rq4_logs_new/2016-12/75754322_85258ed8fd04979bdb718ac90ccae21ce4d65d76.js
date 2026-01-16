/**
 * Created by lixiaoxi on 16/9/14.
 * @description       评论输入框
 *
 */
import React, { Component, PropTypes } from 'react';

class InputBox extends Component {
    $inputDom; // input 框的dom
    phone_width;
    isFocusing = false;
    static propTypes = {
        placeholder: PropTypes.string,
        defaultInputText: PropTypes.string,
        commentCount: PropTypes.number,
        likeCount: PropTypes.number,
        // 点击评论
        onPressComment: PropTypes.func,
        // 点赞
        onPressLike: PropTypes.func,
        // 点击分享
        onPressShare: PropTypes.func,
        // 提交事件
        onSubmit: PropTypes.func,
        sender: PropTypes.func,
        newsLikeCount: PropTypes.func,
    };

    static defaultProps = {
        placeholder: '说点什么吧',
        defaultInputText: '',
        commentCount: 0,
        likeCount: 0,
        onPressComment: () => {},
        onPressLike: () => {},
        onPressShare: () => {},
        onSubmit: () => {},
        newsLikeCount: () => {},
    };
    /* 构造 */
    constructor(props) {
        super(props);
        /* 初始状态 */
        this.state = {
            /* 是否得到焦点 */
            isFocusing: false,
            /* Input 框的值 */
            inputText: props.defaultInputText,
            /* 点赞 */
            isLiked: false,
        };
    }

    componentDidMount() {
        this.$inputDom = this.refs.bottom_input;
        this.$inputBox = this.refs.bottom_left;
        this.$inputBoxRight = this.refs.bottom_right;
        this.phone_width = screen.width;
        const InputBoxBottom = this.refs.InputBoxBottom;
        InputBoxBottom.style.width = (this.phone_width - 20) + 'px';
    }

    componentWillReceiveProps(nextProps) {
        if (nextProps && nextProps.inputText !== this.state.inputText) {
            this.state.inputText = nextProps.defaultInputText || this.state.inputText;
            // this.setState({inputText: nextProps.inputText});
        }
    }

    _onSubmit(e) {
        this.props.onSubmit(this.state.inputText);
        this.setState({ inputText: '', isFocusing: false });
        this.$inputDom.blur();
        e.preventDefault();
        return false;
    }

    _onChange(e) {
        this.setState({ inputText: e.target.value });
    }

    // 失去焦点状态
    _onBlur() {
        this.isFocusing = false;
        this.$inputBox.className = 'dis_bottom_left';
        this.$inputBoxRight.className = 'dis_bottom_right';
        // this.setState({ isFocusing: false });
    }
    // 获取焦点状态
    _onFocus() {
        this.isFocusing = true;
        this.$inputBox.className = 'dis_bottom_left_onfocus';
        this.$inputBoxRight.className = 'dis_bottom_right_onfocus';

        // this.setState({ isFocusing: true });
        // this.state.isFocusing = true;
    }

    /* 点赞待完善 */
    _onPressLike() {
        const newLikeStatus = !this.state.isLiked;
        this.setState({ isLiked: newLikeStatus });
        this.props.onPressLike(newLikeStatus);
        // /* 点赞数传给后台 */
        // let status, channel;
        // if (newLikeStatus === true) {status = 1;}
        // else status = 2;
        // /* 点赞数据传给后台 */
        // this.props.newsLikeCount(this.props.routeId, this.props.userId, 1, status, channel);
    }

    _onPressComment() {
        this.$inputDom.focus();
        // debugger;
        this.props.onPressComment();
        // this.setState({ isFocusing: true });
    }

    _onPressShare() {
        this.props.onPressShare();
    }

    // input固定在底部：样式修改
    _onTouchInput() {
        this.phone_width = screen.width;
        const Boxbottom = this.refs.InputBoxBottom;
        const bottom_top = this.refs.bottom_top;
        if (Boxbottom) {
            if (this.isFocusing) {
                console.log(this.isFocusing, 'this.isFocusing')
                Boxbottom.style.position = 'static';
                Boxbottom.style.width = (this.phone_width - 20) + 'px';
                Boxbottom.style.bottom = 0;
                bottom_top.style.paddingBottom = 0;
            } else {
                Boxbottom.style.position = 'fixed';
                bottom_top.style.paddingBottom = 64 + 'px';
                Boxbottom.style.width = (this.phone_width - 20) + 'px';
            }
        }
    }

    render() {
        console.log(this.state);
        const {
            isFocusing,
            inputText,
            } = this.state;

        const {
            commentCount,
            likeCount,
            placeholder,
        } = this.props;
        return (
            <div>
                <div ref="bottom_top"></div>
                <div className="dis_bottom" ref="InputBoxBottom">
                    <div className="dis_bottom_right" ref="bottom_right" >
                        <span className="dis_bottom_chat" onTouchTap={ ::this._onPressComment }>{this.likeNum(commentCount) }</span>
                        <span className={ this.state.isLiked ? 'dis_bottom_praise_change' : 'dis_bottom_praise'}
                            onTouchEnd={ ::this._onPressLike }
                        >
                            {this.likeNum(likeCount)}
                        </span>
                        <span className="dis_bottom_share" onTouchTap={ ::this._onPressShare}></span>
                    </div>
                    <div className="dis_bottom_left" ref="bottom_left" onTouchTap={this._onTouchInput(isFocusing)}>
                        <form name="searchForm" onSubmit={::this._onSubmit} >
                            <input
                                type="text"
                                className="dis_bottom_input"
                                ref="bottom_input"
                                placeholder={placeholder}
                                onChange={::this._onChange}
                                onBlur={ ::this._onBlur }
                                onFocus={ ::this._onFocus }
                                value={inputText}
                            />
                        </form>
                    </div>
                </div>
            </div>
        );
    }

    /* 点赞数前端显示 */
    likeNum(num) {
        if (num === 0) {
            num = '';
        } else if (num > 9999 && num <= 9999999) {
            num = parseInt(num / 10000, 10);
            num = parseInt(num, 10) + '万';
        } else if (num > 9999999) {
            num = parseInt(num / 10000000, 10);
            num = parseInt(num, 10) + '千万';
        }
        return num;
    }
}

export default InputBox;