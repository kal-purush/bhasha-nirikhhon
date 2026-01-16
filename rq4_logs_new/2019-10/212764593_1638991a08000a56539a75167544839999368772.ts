import { Task } from './task';

export const MOCK_TASK_LIST: Task[] = [
  {
    id: '72d542e0-d7ed-4999-afe9-6920170af1c1',
    name: 'PWAの調査',
    deadline: new Date('2019-08-03 10:30'),
    estimate: 10,
  },
  {
    id: '9ef4acbf-c6c4-4673-b717-1c636a11eb9a',
    name: 'ローカルでPWAを動くか',
    deadline: new Date('2019-08-03 11:00'),
    estimate: 20,
  },
  {
    id: 'b56306b6-4cf4-442a-ac5e-632bef36d2fb',
    name: 'Firebaseの通知ができなくなった',
    deadline: new Date('2019-08-03 15:00'),
    estimate: 30,
  },
  {
    id: '04a4090c-35a4-4116-a192-05cd7ffd3575',
    name: 'Firebaseの調査',
    deadline: new Date('2019-08-03 14:00'),
    estimate: 40,
  },
  {
    id: 'ce9e95f3-574c-4e56-9085-d7c7affa8691',
    name: 'サーバーからほしい情報をまとめる',
    deadline: new Date('2019-08-03 13:30'),
    estimate: 50,
  },
  {
    id: 'b41eba05-e41a-4ffc-8fc1-37ea9256723a',
    name: 'Todoリストのアップデート',
    deadline: new Date('2019-08-03 16:00'),
    estimate: 15,
  },
  {
    id: '0b3d9f0e-a11f-4183-990b-7eb2738435a6',
    name: 'Vue.jsのpush通知の調査',
    deadline: new Date('2019-08-03 10:00'),
    estimate: 55,
  },
  {
    id: 'b917c93f-c63f-4aad-aa5e-66b507c3bdb1',
    name: '通知するTodoを決める',
    deadline: new Date('2019-08-03 16:15'),
    estimate: 70,
  },
  {
    id: 'efba5eae-e4ae-422b-a4c7-c3396007481c',
    name: 'デプロイ環境でのパス問題の解決',
    deadline: new Date('2019-08-03 15:45'),
    estimate: 7,
  },
];