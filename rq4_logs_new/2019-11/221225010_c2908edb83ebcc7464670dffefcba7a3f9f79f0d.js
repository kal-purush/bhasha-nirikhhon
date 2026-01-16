import React from 'react';
import CodeMirror from 'codemirror';
import 'codemirror/mode/markdown/markdown';
import 'codemirror/addon/display/placeholder';
import 'codemirror/lib/codemirror.css'
import 'codemirror/theme/panda-syntax.css';
class CodeMirrorRender extends React.Component {
    constructor() {
        super();
        this.state = {
            value: ""
        }
        this.txtAreaNode = React.createRef();
    }

    componentDidMount = () => {
        const cm = CodeMirror.fromTextArea(this.txtAreaNode.current, {
            mode: 'markdown',
            theme: 'panda-syntax',
            placeholder: '당신의 이야기를 적어보세요...',
            viewportMargin: Infinity,
            lineWrapping: true,
        });

        cm.on("change")
    }

    render() {
        

        return (
            <textarea ref={this.txtAreaNode}></textarea>
        )
    }
}

export default CodeMirrorRender;