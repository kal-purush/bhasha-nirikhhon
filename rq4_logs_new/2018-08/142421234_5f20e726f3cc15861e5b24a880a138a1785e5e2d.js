import Link from 'next/link'
import stylesheet from './bottomNav.less'
import { Picker, List, Toast } from 'antd-mobile';
import arrayTreeFilter from 'array-tree-filter';
import district from '../data/city_data';
import { connect } from 'react-redux';
import * as action from '../../redux/actions';
import { bindActionCreators } from 'redux';
import { Component } from 'react'
import { createForm, formShape } from 'rc-form';

class BottomNav extends Component {
    state = {
        pickerValue: [],
        visible: false,
        one: false,
        two: false,
        three: false,
        bg: false
    };
    static propTypes = {
        form: formShape,
    };
    componentDidMount() {
        this.props.defaultCity(localStorage.getItem('city'))

    }
    submit = (e) => {
        console.log(e)
        this.props.form.validateFields((error, value) => {

            if (JSON.stringify(value.district) == JSON.stringify(["请选择", "请选择", "请选择"])) {
                Toast.fail('请选择您的城市 !!!', 1);
            } else if (value.area == '') {
                Toast.fail('请输入您的房屋面积 !!!', 1);
            } else if (!value.phone.match(/^1[0-9]{10}$/)) {
                Toast.fail('请输入正确的手机号码 !!!', 1);
            } else {
                if (e == 1) {
                    this.props.yanFang({ city: value.district, name: value.area, phone_num: value.phone, source: '设计' })
                    this.setState({ one: false, two: false, bg: false })
                } else {
                    this.props.yanFang({ city: value.district, name: value.area, phone_num: value.phone, source: '报价' })
                    this.setState({ one: false, two: false, bg: false })
                }
            }
        });
    }
    show = (e) => {
        console.log(e)
        this.setState({ bg: true })
        if (e == 1) {
            this.setState({ one: true })
        } else if (e == 2) {
            this.setState({ two: true })
        } else {
            this.setState({ three: true })
        }
    }
    redClick=(e)=>{
        this.props.form.validateFields((error, value) => {
            if(!value.redphone.match(/^1[0-9]{10}$/)){
                Toast.fail('请输入正确的手机号码 !!!', 1);
            }else{
                this.props.yanFang({ city: '装修红包', name:  '装修红包', phone_num: value.redphone, source: '装修红包' })
                this.setState({ one: false, two: false, bg: false,three:false })
            }
        })
    }
    getSel() {
        const value = this.state.pickerValue;
        if (!value) {
            return '';
        }
        const treeChildren = arrayTreeFilter(district, (c, level) => c.value === value[level]);
        return treeChildren.map(v => v.label).join(',');
    }
    render() {
        const navcity = this.props.city ? this.props.city : this.props.default ? this.props.default : 'zhengzhou'
        let errors;
        const { getFieldProps, getFieldError } = this.props.form;
        return (
            <div className='bottomNav'>
                <div className='toptitle' style={this.state.bg ? { display: 'block' } : { display: 'none' }}>
                    <div className='pop-content' style={this.state.three ? { display: 'none' } : { display: 'block' }}>
                        <div className="close_pop" onClick={() => this.setState({ one: false, two: false, bg: false })}>
                            <div className="close_pop_btn"></div>
                        </div>
                        <div className='content-header baojia' style={this.state.one ? { display: 'block' } : { display: 'none' }}>
                            <img src="/static/img/tu_06.png" />
                        </div>
                        <div className='content-header sheji' style={this.state.two ? { display: 'block' } : { display: 'none' }}>
                            <img src="/static/img/tu_08.png" />
                        </div>
                        <div className='mfsj-form'>
                            <div className='row'>

                                <span className='city'>房屋所在地</span>
                                <Picker
                                    visible={this.state.visible}
                                    data={district}
                                    value={this.state.pickerValue}
                                    onChange={v => this.setState({ pickerValue: v })}
                                    onOk={() => this.setState({ visible: false })}
                                    onDismiss={() => this.setState({ visible: false })}
                                    {...getFieldProps('district', {
                                        initialValue: ['请选择', '请选择', '请选择'],
                                    })}
                                >
                                    <List.Item extra={this.getSel()} onClick={() => this.setState({ visible: true })}>

                                    </List.Item>
                                </Picker></div>
                            <div className='row'><span className='area'>房屋面积</span><input {...getFieldProps('area', { initialValue: '' })} placeholder='您的房屋面积' /></div>


                            <div className='row'><span className='phone'>联系方式</span><input {...getFieldProps('phone', { initialValue: '' })} placeholder='请输入您的手机号码' /></div>
                            <button onClick={this.submit.bind(this, 1)} style={this.state.one ? { display: 'block' } : { display: 'none' }}>立即申请免费报价</button>
                            <button onClick={this.submit.bind(this, 2)} style={this.state.two ? { display: 'block' } : { display: 'none' }}>立即申请免费设计</button>
                        </div>


                    </div>

                    <div className='redbao' style={this.state.three?{display:'block'}:{display:'none'}}>
                          <input {...getFieldProps('redphone', { initialValue: '' })} placeholder='请输入您的手机号码' />  
                          <button onClick={this.redClick}> </button>
                          <div className='closed' onClick={()=>(this.setState({ one: false, two: false, bg: false,three:false }))}>
                                    
                          </div>
                    </div>
                </div>

                <div className='centerNav' >
                    <a onClick={this.show.bind(this, 1)}>6秒报价</a>
                    <a onClick={this.show.bind(this, 2)}>免费设计</a>
                    <a href={`/${navcity}`}>口碑公司</a>
                    <a onClick={this.show.bind(this, 3)}>装修红包</a>
                </div>
                <style>{stylesheet}</style>
            </div >
        )
    }
}


function mapStateToProps(state) {
    return { default: state.defaultCity }
}
function mapDispatchToProps(dispatch) {
    return {
        ...bindActionCreators(action, dispatch)
    }
}
const Index = createForm()(BottomNav);
export default connect(mapStateToProps, mapDispatchToProps)(Index)