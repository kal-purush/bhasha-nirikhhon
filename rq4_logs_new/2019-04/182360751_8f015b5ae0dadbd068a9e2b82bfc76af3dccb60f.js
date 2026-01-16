
import { Terminal } from 'xterm';
import * as fullscreen from 'xterm/lib/addons/fullscreen/fullscreen';
import fromUTF8Array from './TextConvertor';
import 'xterm/dist/addons/fullscreen/fullscreen.css';
import 'xterm/dist/xterm.css';


Terminal.applyAddon(fullscreen);


function createLogWindow(wsUrl) {
  const term = new Terminal({
    cols: 200,
    cursorBlink: false,
    useStyle: true,
    fontSize: 12,
  });
  term.open(document.getElementById('container-shell'));

  const ws = new WebSocket(wsUrl);
  ws.binaryType = 'arraybuffer';

  ws.onmessage = (event) => {
    if (typeof (event.data) === 'string') {
      console.log(event.data);
      return;
    }

    const str = fromUTF8Array(new Uint8Array(event.data));
    term.write(str.replace(/\n/g, '\r\n'));
  };

  window.term = term;
  term.setOption('disableStdin', true);
  term.focus();
}

export default createLogWindow;