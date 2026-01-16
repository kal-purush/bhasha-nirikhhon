class GameController {
    constructor() {
        this.model = new GameModel();
        this.generate_next_form();
        this.timer = 0.0;
    }
    update(dt) {
        let frequency = 1.0;
        this.timer += dt;
        if(this.timer >= frequency){
            this.timer -= frequency;
            this.model.current_figure.j -= 1;
            if(this.has_collision()){
                this.model.current_figure.j += 1;
                this.next_figure()
            }
        }
    }

    next_figure(){
        this.join_current_figure();
        this.find_matches();
        this.generate_next_form();
    }

    key_down(ev) {
        let left = () => {
            this.model.current_figure.i -=1;
        };

        let right = () => {
            this.model.current_figure.i +=1
        };

        let down = () => {
            this.model.current_figure.j -=1
        };

        let up = () => {
            this.model.current_figure.j +=1
        };

        let rotate_right = () => {
            this.rotate_right()
        };

        let rotate_left = () => {
            this.rotate_left()
        };

        let offsets = {
            'ArrowLeft': [left, right],
            'ArrowRight': [right, left],
            'ArrowDown': [down, up],
            'ArrowUp': [rotate_right, rotate_left],
        };

        if(ev.key in offsets) {
            console.log('ev in offsets');
            let [action, undo] = offsets[ev.key];
            action();

            if(this.has_collision()) {
                undo();
            }
        }
    }

    join_current_figure(){
        for(let [i, j] of this.model.current_figure.coords){
            this.model.cells[i+this.model.current_figure.i][j+this.model.current_figure.j] = this.model.current_figure.color;
        }
    }
    generate_next_form(){
        this.model.current_figure = new Figure(this.model.shuffle.pop_figure());
        this.rotate_random();
        this.model.next_figure = new Figure(this.model.shuffle.get_next_figure());

        let coords = this.model.current_figure.get_world_coords();
        while(coords.some(([i, j]) => j >= this.model.height)){
            this.model.current_figure.j -=1;
            coords = this.model.current_figure.get_world_coords();
        }
        if(this.has_collision()){
            this.finish_game();
        }
    }

    has_collision() {
        let coords = this.model.current_figure.get_world_coords();
        if (coords.every(([i, j]) =>
            i >= 0 &&
            i < this.model.width &&
            j >= 0 &&
            j < this.model.height &&
            this.model.cells[i][j] === CELL_EMPTY))
        {
            console.log('все норм', coords, coords.every(([i, j]) => i >= 0 && i < this.model.width && j >= 0));
            return false;
        } else {
            console.log('коллизия', coords, coords.every(([i, j]) => i >= 0 && i < this.model.width && j >= 0));
            return true;
        }
    }

    find_matches(){
        // TODO: find_matches
    }

    finish_game(){
        // TODO: finish_game
    }

    rotate_random(){
        let rand = Math.floor(Math.random() * 4);
        for(let i=0; i<rand; ++i) {
            this.rotate_right();
        }
    }

    rotate_right(){
        this.model.current_figure.coords.forEach((coord, index) => {
            this.model.current_figure.coords[index] = [coord[1], -coord[0]];
        });
    }

    rotate_left(){
        this.model.current_figure.coords.forEach((coord, index) => {
            this.model.current_figure.coords[index] = [-coord[1], coord[0]];
        });
    }
}