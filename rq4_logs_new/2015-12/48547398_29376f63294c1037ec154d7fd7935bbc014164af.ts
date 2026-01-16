module TwitchPotato {

    export class GuideKeybinds {

        constructor() {
            this.CreateKeybinds();
        }

        /** Handles input for the menu. */
        OnInput(input: Inputs): boolean {

            if (!App.Guide.isOpen || !this._isOpen) return false;

            switch (input) {
                case Inputs.Left:
                case Inputs.Right:
                    this.Close();
                    return true;

                case Inputs.Up:
                    this.Scroll(Direction.Up);
                    return true;

                case Inputs.Down:
                    this.Scroll(Direction.Down);
                    return true;

                default:
                    return false;
            }
        }

        Open(): void {
            setTimeout(() => this.Scroll(Direction.None), 100);
            $(this._bindingMenu).removeClass('closed');
            this._isOpen = true;
        }

        Close(): void {
            $(this._bindingMenu).addClass('closed');
            this._isOpen = false;
        }

        private CreateKeybinds(): void {
            var html = '';
            var bindingHtml =
                '<div class="binding">' +
                '<div class="key">{0}</div>' +
                '<div class="desc">{1}</div>' +
                '</div>';

            var inputs: Array<Input> = App.Input.Inputs;

            for (var i in inputs) {
                var input: Input = inputs[i];
                html += bindingHtml.format(input.key, input.desc);
            }

            $(this._bindingMenu).append(html);
        }

        private Scroll(direction: Direction): void {
            var index = $(this._bindingMenu)
                .find('.binding')
                .index($(this._selectedBind));

            if (index === -1)
                index = 0;

            if (direction === Direction.Down)
                index++;
            else if (direction === Direction.Up)
                index--;

            var numBinds = $(this._bindingMenu).find('.binding').length - 1;

            if (index < 0)
                index = 0;
            if (index > numBinds)
                index = numBinds;

            $(this._selectedBind).removeClass('selected');

            $(this._bindingMenu)
                .find('.binding')
                .eq(index)
                .addClass('selected');

            $(this._bindingMenu).scrollToMiddle(this._selectedBind);
        }

        private _bindingMenu = '#keybinds-menu';
        private _selectedBind = '#keybinds-menu .binding.selected';

        private _isOpen = false;
        get IsOpen(): boolean { return this._isOpen; }
    }
}