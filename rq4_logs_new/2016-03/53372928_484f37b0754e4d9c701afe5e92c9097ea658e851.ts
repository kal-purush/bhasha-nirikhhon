module News {

    /**
    * Contains all the patient data
    */
    export class BulletinBoard {
        /**
        * Gets a string value for AUTHOR
        */
        public get AUTHOR(): string {
            return this.author;
        };

        /**
        * Sets a string value for AUTHOR
        */
        public set AUTHOR(value: string) {
            this.author = value;
        };
        /**
        * Gets a string value for AUTHOR
        */
        public get BODY(): string {
            return this.body;
        };

        /**
        * Sets a string value for AUTHOR
        */
        public set BODY(value: string) {
            this.body = value;
        };

        //Stores author info
        private author: string = "";
        //Stores body info
        private body: string = "";

        //the html row element
        public rowElement: HTMLElement;

        /**
        * Ctor
        */
        constructor(info: BulletinBoard) {
            if (info === null) return;

            this.author = info.author;
            this.body = info.body;
        }
    }

    /**
    * The instance used to populate the news bulletin window with the user updates
    */
    export var BulletinInfo: BulletinBoard[] = [];

    export function Load(): void {
        var newInstance = $('#news-template').clone(true, true);
        newInstance.attr('style', '');

        $('#news-list').append(newInstance);

        var newsRowIndex = $("#news-list").children().length - 2;
        var rowId = 'news-row-' + newsRowIndex;

        newInstance.attr('id', rowId);

        var $row = $("#" + rowId);

        var userInfo = BulletinInfo[newsRowIndex] ? BulletinInfo[newsRowIndex] : AddNewEntry();

        userInfo.rowElement = newInstance.get(0);
        $row.find(".news-author").html(userInfo.AUTHOR);
        $row.find(".news-body").html(userInfo.BODY);
    }

    /**
    * Adds a new entry to the bulletin board
    */
    export function AddNewEntry(): BulletinBoard {
        var userInfo = new BulletinBoard(null);

        BulletinInfo.push(userInfo);

        return userInfo;
    }

    /**
    * Deletes a news article from the bulletin list
    * @param rowIndex: deletes the patient row by index (its 0-based)
    */
    export function DeleteRow(rowIndex: number): void {
        var foundIndex = false;

        for (var index = 0; index <= BulletinBoard.length; index++) {
            var rowId = $('#' + 'news-row-' + index);
            if (rowIndex === index) {
                foundIndex = true;
                rowId.remove();
                continue;
            }

            var indexId = foundIndex ? index - 1 : index;

            rowId.attr('id', 'news-row-' + index);
        }
        BulletinInfo.splice(rowIndex, 1);
    }

    /**
    * Deletes all 
    * the news article from the bulletin board
    */
    export function DeleteAllData(): void {
        var $row = $('#news-template').siblings();

        for (var indx = 0; indx < $row.length; indx++) {
            $row[indx].remove();
        }
    }
} 