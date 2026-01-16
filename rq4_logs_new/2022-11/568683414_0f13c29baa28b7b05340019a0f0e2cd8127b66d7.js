$(document).ready(() => {
    $('.game-display h4').click(() => {
        $('.game-display h4').css('display', 'none');
        $('.game-display').css('background-image', 'none');

        const getArticle = $('.new-article');
        getArticle.css('width', '100%');
  
        const setHead = $('h3').text('CHOOSE GAME MODE');
        const setP1 = $('<p>');
        const setCheckbox1 = setP1.append($('<input>', {type: 'radio', id: 'AI', value: 'AI', name: 'game-mode'}));
        const setLabel1 = setP1.append($('<label>', {for: 'AI'}).text('Player 1 vs AI'));
        const setP2 = $('<p>');
        const setCheckbox2 = setP2.append($('<input>', {type: 'radio', id: 'player', value: 'P2', name: 'game-mode'}));
        const setLabel2 = setP2.append($('<label>', {for: 'player'}).text('Player 1 vs Player 2'));
        const setButton = $('<button>', {type: 'button', class: 'button'}).text('Choose');
        getArticle.append([setHead, setCheckbox1, setLabel1, setCheckbox2, setLabel2, setButton]);
        const getRadio = $('input')
        getRadio.click(() => {
            getRadio.each((index, data) => {
                if ($(data).is(':checked')) {
                    // console.log($(data).attr('value'));
                    getValue = $(data).attr('value');
                    // console.log(getValue);
                }
            });
        })
        $('.new-article button').click(() => {
            if (getValue) {
                $('h3').text(`Player 1 vs ${eval(getValue).name}`);
                $('button, label, input').css('display', 'none');
                $('.game-display div').css('display', 'grid');
                $('.game-display div div').css('display', 'block');
                $('.game-display').css('background-color', 'purple');

                const getScoreboard = $('.scoreboard');
                getScoreboard.css('background-image', 'none');
            }
        });
    });
});