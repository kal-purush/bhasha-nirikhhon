let cards = $("#cards");
let pixel_offset = 0;
let all_cards = cards.children();
const offset = (all_cards.length - 1) / 2;
let btn_index = offset;
let current_card = all_cards[btn_index];

function handle_move() {
    cards.attr("style", `margin-left: ${pixel_offset}px;`);

    current_card = all_cards[btn_index];

    current_card.setAttribute("main-card", "true");
}

function next() {
    pixel_offset -= 1000;
    if (Math.abs(pixel_offset) / 1000 > all_cards.length - 2) {
        pixel_offset = Math.abs(pixel_offset) - 1000;
    }

    current_card.removeAttribute("main-card");
    btn_index++;

    if (btn_index >= all_cards.length) {
        btn_index = 0;
    }
}

function prev() {
    pixel_offset += 1000;
    if (pixel_offset / 1000 > all_cards.length - 2) {
        pixel_offset = -pixel_offset + 1000;
    }

    current_card.removeAttribute("main-card");
    btn_index--;

    if (btn_index < 0) {
        btn_index = all_cards.length - 1;
    }
}

for (let i = 0; i < 1000; i++) {
    setTimeout(function() {
        next();
        handle_move();
    }, 5000*(i+1));
}