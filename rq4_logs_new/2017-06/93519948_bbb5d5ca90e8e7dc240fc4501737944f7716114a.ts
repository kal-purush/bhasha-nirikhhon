
import * as Mock from 'mockjs';

import { BASE_URL } from './mock-config';


Mock.mock(`${BASE_URL}/issues`, {
    'data|15': [{
        'id|+1': 1,
        'name': '@ctitle(6, 20)',
        'status|1': ['未开始', '进行中', '已完成', '已取消', '已关闭'],
        'createdBy': {'id': 1, 'name': '@cname', 'displayName': '@cname'},
        'createdDate': '@date'
    }]
});


Mock.mock(new RegExp(`${BASE_URL}/issues/\\d$`), {
    'data': {
        'id': 1,
        'name': '@ctitle(6, 20)',
        'status|1': ['未开始', '进行中', '已完成', '已取消', '已关闭'],
        'createdBy': {'id': 1, 'name': '@cname', 'displayName': '@cname'},
        'createdDate': '@date'
    }
});