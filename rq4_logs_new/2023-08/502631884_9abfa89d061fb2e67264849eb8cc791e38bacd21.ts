import {createApp} from 'vue';
import Toast from './Toast.vue';

let isToasting = false;

export default function toast(message: string) {
    if (isToasting) return;
    isToasting = true;
    const mountNode = document.createElement('div');
    document.body.appendChild(mountNode);
    const app = createApp(Toast, {
        message,
        close: () => {
            console.log('toast close function done ===== ');
            app.unmount();
            document.body.removeChild(mountNode);
            isToasting = false;
        }
    });
    app.mount(mountNode);
}