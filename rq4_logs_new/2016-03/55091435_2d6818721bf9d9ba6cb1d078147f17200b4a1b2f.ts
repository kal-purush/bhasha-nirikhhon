import {async, register} from 'platypus';
import BaseRepository from '../base/base.repo';

export default class CalcRepository extends BaseRepository {

}

register.injectable('calc-repo', CalcRepository);