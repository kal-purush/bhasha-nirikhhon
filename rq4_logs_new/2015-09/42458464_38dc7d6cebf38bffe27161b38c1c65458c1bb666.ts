export module NameValidator {
    export interface IValidator {
        isValidate(input: string): boolean;
    }

    export class StringValidator implements IValidator {

        constructor() {

        }

        public isValidate(input: string): boolean {
            return true;
        }
    }

    /**
     *
     */
    export class TableNameValidator extends StringValidator {

        constructor() {
            super();
        }

        private moreValidate(input: string): boolean {
            return (input.indexOf('-') == -1);
        }

        /**
         *
         * @param input
         * @returns {boolean}
         */
        public isValidate(input: string): boolean {
            /**
             *
             */
            return (super.isValidate(input) && this.moreValidate(input));
        }
    }
}