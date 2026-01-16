/*
 * Initial for quill -- rich text editor
 */
var Embed = Quill.import('blots/block/embed');
class Hr extends Embed {
    static create(value) {
        let node = super.create(value);
        // give it some margin
        // node.setAttribute('style', "height:0px; border: 2px solid #ededed; margin-top:10px; margin-bottom:10px;");
        return node;
    }
}

Hr.blotName = 'hr'; //now you can use .ql-hr classname in your toolbar
// Hr.className = 'my-hr';
Hr.tagName = 'hr';

var customHrHandler = function(){
    var range = quill.getSelection();
    if (range) {
        quill.insertEmbed(range.index, "hr", "null")
    }
}

var toolbarOptions = [
    [{ 'header': [1, 2, 3, 4, false] }],                    // 4, 5, 6,
    // [{ 'font': [] }],
    ['bold', 'italic', 'underline'],                     // toggled buttons    'strike'
    // ['blockquote', 'code-block'],                     // 代码块
    // [{ 'header': 1 }, { 'header': 2 }],               // custom button values
    [{ 'list': 'ordered'}, { 'list': 'bullet' }],
    // [{ 'script': 'sub'}, { 'script': 'super' }],         // superscript/subscript
    // [{ 'indent': '-1'}, { 'indent': '+1' }],          // outdent/indent
    // [{ 'direction': 'rtl' }],                         // text direction
    // [{ 'size': ['small', false, 'large', 'huge'] }],  // custom dropdown
    // [{ 'color': [] }, { 'background': [] }],             // dropdown with defaults from theme
    ['link', 'image'],                                            // 插入链接、图片
    ['et-img'],
    [{'hr': customHrHandler}],
    // ['formula'],
    // [{ 'align': [] }],                                // 暂时不先挂拍
    ['clean']                                            // remove formatting button
];

Quill.register({
    'formats/hr': Hr
});

var quill = new Quill('#editor', {
    modules: { toolbar: toolbarOptions },
    theme: 'snow'
});

var toolbarOptions2 = [
    ['bold', 'italic', 'underline'],                     // toggled buttons    'strike'
    [{ 'color': [] }, { 'background': [] }],             // dropdown with defaults from theme
    ['clean']
];

var quill2 = new Quill('#editor2', {
    modules: { toolbar: toolbarOptions2 },
    theme: 'snow'
});

var et = document.querySelector('.ql-et-img');
var etPopoverImg;
var etPopoverPos;
et.addEventListener('click', () => {
    etPopoverPos = quill.getSelection();
    etPopoverImg = quill.getContents(etPopoverPos);
    console.log(etPopoverImg);
    etPopover.style.display = 'block';
});

// /**
//  * play a trick
//  * 
//  * .et-pop
//  *   &::before
//  *   input
//  *   label::after
//  *   input
//  *   a#et-pop-action.ql-action:after
//  */
var etPopover = document.createElement('div');
etPopover.style.display = 'none';
etPopover.classList.add('et-pop');
etPopover.classList.add('ql-tooltip');
etPopover.classList.add('ql-editing');
etPopover.innerHTML = '<input name="et-pop-width" type="text" value="321"/><label></label><input name="et-pop-alt" type="text" value="图1" /><a id="et-pop-action" class="ql-action"></a>';

$('#editor').append(etPopover);
$('.ql-et-img').append('<svg id="et-pop-svg" viewBox="0 0 18 18"><rect class="ql-stroke et-pop-svg-stroke" height="10" width="12" x="3" y="4" fill="purple"></rect><circle class="ql-fill et-pop-svg-fill" cx="6" cy="7" r="1"></circle><polyline class="ql-even ql-fill et-pop-svg-fill" points="5 12 5 11 7 9 8 10 11 7 13 9 13 12 5 12"></polyline> </svg>');

document.getElementById('et-pop-action').addEventListener('click', function() {
    quill.setSelection(etPopoverPos);
    quill.format('width', $('input[name=et-pop-width]').val() + 'px');
    quill.format('alt', $('input[name=et-pop-alt]').val() + '');
    etPopover.style.display = 'none';
});

// {"insert":{"hr":true}}