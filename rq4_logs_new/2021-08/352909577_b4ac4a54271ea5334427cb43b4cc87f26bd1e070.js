import $ from 'jquery';
import colors from './files/colors.json';
import './css/settings.css';

export async function createSettings() {
    const data = await browser.storage.sync.get('color');
    const prevId = data.color ? data.color : 0;
    setColor(prevId);

    const gradientList = colors.map((c) =>
        $('<div>')
            .addClass(`color-option ${prevId === c.id ? 'active-color' : ''}`)
            .on('click', changeColor.bind(this, c.id))
            .css('--start', c.start)
            .css('--end', c.end)
    );

    const formWrapper = $('<div>').attr('id', 'form-wrapper').append(gradientList);
    $('#settings-wrapper').empty().append(formWrapper);
}

async function changeColor(id) {
    setColor(id)
    await browser.storage.sync.set({ color: id });
    createSettings();
}

function setColor(id) {
    const color = colors.find((c) => c.id === id);
    $('body').css('--start', color.start).css('--end', color.end);
}