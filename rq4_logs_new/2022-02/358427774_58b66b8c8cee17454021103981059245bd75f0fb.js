function setup() {
    let canvas = createCanvas(1200,700);
    canvas.parent("game");
    initializeFields();
    createStars();
}

function draw() {
    background(20);
    twinklingStars();
    mountains();
    foreground();
    pointing();
    all_telescopes();
    stickman();
    // atmosphere();
    // buttons();
    control_buttons()
    if (play){
        H -= Wsky
    }

    t0++;
    clicked = false;
}