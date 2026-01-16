import Component from 'vue-class-component';
import {MENU} from '../../../common/constant';
import store from '../../../store/index';
import CompomentBase from '../../ComponentBase';

@Component({
  template: require('./MemberList.html'),
  name: 'MemberList',
})

/**
 * MemberListコンポーネント
 */
export default class MemberList extends CompomentBase {

  public mounted(): void {
  }

}