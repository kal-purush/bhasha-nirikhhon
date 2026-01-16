module HtmlDiff {

    export function Execute(oldText: string, newText: string): string {
        return new HtmlDiff(oldText, newText).Build();
    }

    class HtmlDiff {
        private _content: string;
        private _newText: string; // readonly
        private _oldText: string; // readonly

        private _specialCaseClosingTags: string[] = ["</strong>", "</b>", "</i>", "</big>", "</small>", "</u>", "</sub>", "</sup>", "</strike>", "</s>"];
        private _specialCaseOpeningTags: string[] = ["<strong[\\>\\s]+", "<b[\\>\\s]+", "<i[\\>\\s]+", "<big[\\>\\s]+", "<small[\\>\\s]+", "<u[\\>\\s]+", "<sub[\\>\\s]+", "<sup[\\>\\s]+", "<strike[\\>\\s]+", "<s[\\>\\s]+"];
        private static SpecialCaseWordTags: string[] = ["<img"];

        private _newWords: string[];
        private _oldWords: string[];
        private _wordIndices: { [word: string]: number[] };

        /// <param name="oldText">The old text.</param>
        /// <param name="newText">The new text.</param>
        constructor(oldText: string, newText: string) {
            this._oldText = oldText;
            this._newText = newText;

            this._content = '';
        }

        /// <returns>HTML diff markup</returns>
        public Build(): string {
            var self = this;
            this.SplitInputsToWords();
            this.IndexNewWords();
            var operations: Operation[] = this.Operations();

            operations.forEach((item: Operation) => {
                self.PerformOperation(item);
            });

            return this._content;
        }

        private IndexNewWords(): void {
            this._wordIndices = {};
            for (var i: number = 0; i < this._newWords.length; i++) {
                var word: string = this._newWords[i];

                // if word is a tag, we should ignore attributes as attribute changes are not supported (yet)
                if (HtmlDiff.IsTag(word)) {
                    word = HtmlDiff.StripTagAttributes(word);
                }

                if (this._wordIndices[word]) {
                    this._wordIndices[word].push(i);
                }
                else {
                    this._wordIndices[word] = [i];
                }
            }
        }

        private static StripTagAttributes(word: string): string {
            return /^\s*<\s*([^\s=]*)/.exec(word)[0];
        }

        private SplitInputsToWords(): void {
            this._oldWords = this.ConvertHtmlToListOfWords(HtmlDiff.Explode(this._oldText));
            this._newWords = this.ConvertHtmlToListOfWords(HtmlDiff.Explode(this._newText));
        }

        private ConvertHtmlToListOfWords(characterString: string[]): string[] {
            var self = this;
            var mode: Mode = Mode.Character;
            var currentWord: string = '';
            var words: string[] = [];

            characterString.forEach(character => {
                switch (mode) {
                    case Mode.Character:

                        if (HtmlDiff.IsStartOfTag(character)) {
                            if (currentWord !== '') {
                                words.push(currentWord);
                            }

                            currentWord = "<";
                            mode = Mode.Tag;
                        }
                        else if (/\s/.test(character)) {
                            if (currentWord !== '') {
                                words.push(currentWord);
                            }
                            currentWord = character;
                            mode = Mode.Whitespace;
                        }
                        else if (/[\w\#@]+/i.test(character)) {
                            currentWord += character;
                        }
                        else {
                            if (currentWord !== '') {
                                words.push(currentWord);
                            }
                            currentWord = character;
                        }

                        break;
                    case Mode.Tag:

                        if (HtmlDiff.IsEndOfTag(character)) {
                            currentWord += ">";
                            words.push(currentWord);
                            currentWord = "";

                            mode = HtmlDiff.IsWhiteSpace(character) ? Mode.Whitespace : Mode.Character;
                        }
                        else {
                            currentWord += character;
                        }

                        break;
                    case Mode.Whitespace:

                        if (HtmlDiff.IsStartOfTag(character)) {
                            if (currentWord !== '') {
                                words.push(currentWord);
                            }
                            currentWord = "<";
                            mode = Mode.Tag;
                        }
                        else if (/\\s/.test(character)) {
                            currentWord += character;
                        }
                        else {
                            if (currentWord !== '') {
                                words.push(currentWord);
                            }

                            currentWord = character;
                            mode = Mode.Character;
                        }

                        break;
                }
            });
            if (currentWord !== '') {
                words.push(currentWord);
            }

            return words;
        }

        private PerformOperation(operation: Operation): void {
            var self = this;
            switch (operation.Action) {
                case Action.Equal:
                    self.ProcessEqualOperation(operation);
                    break;
                case Action.Delete:
                    self.ProcessDeleteOperation(operation, "diffdel");
                    break;
                case Action.Insert:
                    self.ProcessInsertOperation(operation, "diffins");
                    break;
                case Action.None:
                    break;
                case Action.Replace:
                    self.ProcessReplaceOperation(operation);
                    break;
            }
        }

        private ProcessReplaceOperation(operation: Operation): void {
            this.ProcessDeleteOperation(operation, "diffmod");
            this.ProcessInsertOperation(operation, "diffmod");
        }

        private ProcessInsertOperation(operation: Operation, cssClass: string): void {
            this.InsertTag("ins", cssClass,
                this._newWords.filter((s, pos) => pos >= operation.StartInNew && pos < operation.EndInNew));
        }

        private ProcessDeleteOperation(operation: Operation, cssClass: string): void {
            var text: string[] =
                this._oldWords.filter((s, pos) => pos >= operation.StartInOld && pos < operation.EndInOld);
            this.InsertTag("del", cssClass, text);
        }

        private ProcessEqualOperation(operation: Operation): void {
            var result: string[] =
                this._newWords.filter((s, pos) => pos >= operation.StartInNew && pos < operation.EndInNew);
            this._content += result.join("");
        }


        /// <summary>
        ///     This method encloses words within a specified tag (ins or del), and adds this into "content",
        ///     with a twist: if there are words contain tags, it actually creates multiple ins or del,
        ///     so that they don't include any ins or del. This handles cases like
        ///     old: '<p>a</p>'
        ///     new: '<p>ab</p>
        ///     <p>
        ///         c</b>'
        ///         diff result: '<p>a<ins>b</ins></p>
        ///         <p>
        ///             <ins>c</ins>
        ///         </p>
        ///         '
        ///         this still doesn't guarantee valid HTML (hint: think about diffing a text containing ins or
        ///         del tags), but handles correctly more cases than the earlier version.
        ///         P.S.: Spare a thought for people who write HTML browsers. They live in this ... every day.
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="cssClass"></param>
        /// <param name="words"></param>
        private InsertTag(tag: string, cssClass: string, words: string[]): void {
            while (true) {
                if (words.length === 0) {
                    break;
                }

                var nonTags: string[] = this.ExtractConsecutiveWords(words, x => !HtmlDiff.IsTag(x));

                var specialCaseTagInjection: string = '';
                var specialCaseTagInjectionIsBefore: boolean = false;

                if (nonTags.length !== 0) {
                    var text: string = HtmlDiff.WrapText(nonTags.join(""), tag, cssClass);

                    this._content += (text);
                }
                else {
                    // Check if strong tag
                    if (FirstOrDefault(this._specialCaseOpeningTags, x => new RegExp(x).test(words[0])) !== null) {
                        specialCaseTagInjection = "<ins class='mod'>";
                        if (tag === "del") {
                            words.shift();
                        }
                    }
                    else if (this._specialCaseClosingTags.some(mine => mine === words[0])) {
                        specialCaseTagInjection = "</ins>";
                        specialCaseTagInjectionIsBefore = true;
                        if (tag === "del") {
                            words.shift();
                        }
                    }
                }

                if (words.length === 0 && specialCaseTagInjection.length === 0) {
                    break;
                }

                if (specialCaseTagInjectionIsBefore) {
                    this._content += specialCaseTagInjection + this.ExtractConsecutiveWords(words, HtmlDiff.IsTag).join("");
                }
                else {
                    this._content += this.ExtractConsecutiveWords(words, HtmlDiff.IsTag).join("") + specialCaseTagInjection;
                }
            }
        }

        private ExtractConsecutiveWords(words: string[], condition: (str: string) => boolean): string[] {
            var indexOfFirstTag: number = null;

            for (var i: number = 0; i < words.length; i++) {
                var word: string = words[i];

                if (!condition(word)) {
                    indexOfFirstTag = i;
                    break;
                }
            }

            if (indexOfFirstTag !== null) {
                var items: string[] = words.filter((s, pos) => pos >= 0 && pos < indexOfFirstTag);
                if (indexOfFirstTag > 0) {
                    words.splice(0, indexOfFirstTag);
                }
                return items;
            }
            else {
                var items: string[] = words.filter((s, pos) => pos >= 0 && pos <= words.length);
                words.splice(0, words.length);
                return items;
            }
        }

        private Operations(): Operation[] {
            var positionInOld: number = 0, positionInNew = 0;
            var operations: Operation[] = [];

            var matches: Match[] = this.MatchingBlocks();

            matches.push(new Match(this._oldWords.length, this._newWords.length, 0));

            matches.forEach((match: Match) => {
                var matchStartsAtCurrentPositionInOld: boolean = (positionInOld === match.StartInOld);
                var matchStartsAtCurrentPositionInNew: boolean = (positionInNew === match.StartInNew);

                var action: Action;

                if (matchStartsAtCurrentPositionInOld === false
                    && matchStartsAtCurrentPositionInNew === false) {
                    action = Action.Replace;
                }
                else if (matchStartsAtCurrentPositionInOld
                    && matchStartsAtCurrentPositionInNew === false) {
                    action = Action.Insert;
                }
                else if (matchStartsAtCurrentPositionInOld === false) {
                    action = Action.Delete;
                }
                else // This occurs if the first few words are the same in both versions
                {
                    action = Action.None;
                }

                if (action !== Action.None) {
                    operations.push(
                        new Operation(action,
                            positionInOld,
                            match.StartInOld,
                            positionInNew,
                            match.StartInNew));
                }

                if (match.Size !== 0) {
                    operations.push(new Operation(
                        Action.Equal,
                        match.StartInOld,
                        match.EndInOld,
                        match.StartInNew,
                        match.EndInNew));
                }

                positionInOld = match.EndInOld;
                positionInNew = match.EndInNew;
            });

            return operations;
        }

        private MatchingBlocks(): Match[] {
            var matchingBlocks: Match[] = [];
            this.FindMatchingBlocks(0, this._oldWords.length, 0, this._newWords.length, matchingBlocks);
            return matchingBlocks;
        }


        private FindMatchingBlocks(startInOld: number, endInOld: number, startInNew: number, endInNew: number,
            matchingBlocks: Match[]): void {
            var match: Match = this.FindMatch(startInOld, endInOld, startInNew, endInNew);

            if (match !== null) {
                if (startInOld < match.StartInOld && startInNew < match.StartInNew) {
                    this.FindMatchingBlocks(startInOld, match.StartInOld, startInNew, match.StartInNew, matchingBlocks);
                }

                matchingBlocks.push(match);

                if (match.EndInOld < endInOld && match.EndInNew < endInNew) {
                    this.FindMatchingBlocks(match.EndInOld, endInOld, match.EndInNew, endInNew, matchingBlocks);
                }
            }
        }


        private FindMatch(startInOld: number, endInOld: number, startInNew: number, endInNew: number): Match {
            var bestMatchInOld: number = startInOld;
            var bestMatchInNew: number = startInNew;
            var bestMatchSize: number = 0;

            var matchLengthAt: { [key: number]: number } = {};

            for (var indexInOld: number = startInOld; indexInOld < endInOld; indexInOld++) {
                var newMatchLengthAt: { [key: number]: number } = {};

                var index: string = this._oldWords[indexInOld];

                if (HtmlDiff.IsTag(index)) // strip attributes as this is not supported (yet)
                {
                    index = HtmlDiff.StripTagAttributes(index);
                }

                if (!(index in this._wordIndices)) {
                    matchLengthAt = newMatchLengthAt;
                    continue;
                }

                this._wordIndices[index].forEach(indexInNew => {
                    if (indexInNew < startInNew) {
                        return;
                    }

                    if (indexInNew >= endInNew) {
                        return; // break;
                    }

                    var newMatchLength: number = ( ((indexInNew - 1) in matchLengthAt) ? matchLengthAt[indexInNew - 1] : 0) +
                        1;
                    newMatchLengthAt[indexInNew] = newMatchLength;

                    if (newMatchLength > bestMatchSize) {
                        bestMatchInOld = indexInOld - newMatchLength + 1;
                        bestMatchInNew = indexInNew - newMatchLength + 1;
                        bestMatchSize = newMatchLength;
                    }
                });

                matchLengthAt = newMatchLengthAt;
            }

            return bestMatchSize !== 0 ? new Match(bestMatchInOld, bestMatchInNew, bestMatchSize) : null;
        }

        private static WrapText(text: string, tagName: string, cssClass: string): string {
            return Format("<{0} class='{1}'>{2}</{0}>", tagName, cssClass, text);
        }

        private static IsTag(item: string): boolean {
            if (HtmlDiff.SpecialCaseWordTags.some(re => item !== null && StartsWith(item, re))) return false;
            return HtmlDiff.IsOpeningTag(item) || HtmlDiff.IsClosingTag(item);
        }

        private static IsOpeningTag(item: string): boolean {
            return /^\\s*<[^>]+>\\s*$/.test(item);
        }

        private static IsClosingTag(item: string): boolean {
            return /^\\s*<\/[^>]+>\\s*$/.test(item);
        }

        private static IsStartOfTag(val: string): boolean {
            return val === "<";
        }

        private static IsEndOfTag(val: string): boolean {
            return val === ">";
        }

        private static IsWhiteSpace(value: string): boolean {
            return /\\s/.test(value);
        }

        private static Explode(value: string): string[] {
            return value.split('');
        }
    }

    export class Match {
        public constructor(startInOld: number, startInNew: number, size: number) {
            this.StartInOld = startInOld;
            this.StartInNew = startInNew;
            this.Size = size;
        }

        public StartInOld: number;
        public StartInNew: number;
        public Size: number;
        public get EndInOld(): number {
            return this.StartInOld + this.Size;
        }
        public get EndInNew(): number {
            return this.StartInNew + this.Size;
        }
    }

    export class Operation {
        public constructor(action: Action, startInOld: number, endInOld: number, startInNew: number, endInNew: number) {
            this.Action = action;
            this.StartInOld = startInOld;
            this.EndInOld = endInOld;
            this.StartInNew = startInNew;
            this.EndInNew = endInNew;
        }

        public Action: Action;
        public StartInOld: number;
        public EndInOld: number;
        public StartInNew: number;
        public EndInNew: number;

    }


    enum Mode {
        Character,
        Tag,
        Whitespace,
    }

    enum Action {
        Equal,
        Delete,
        Insert,
        None,
        Replace
    }

    function StartsWith(str: string, regExp: string) {
        return (new RegExp("^" + regExp)).test(str.toString());
    }

    function EndsWith(str: string, regExp: string) {
        return (new RegExp(regExp + "$")).test(str.toString());
    }

    function Format(str: string, ...params: string[]) {
        return str.replace(/{(\d+)}/g, function(match, number) { 
            return typeof params[number] != 'undefined'
                ? params[number]
                : match
                ;
        });
    }

    function FirstOrDefault<T>(array: T[], predicate: (item: T) => boolean): T {
        var filtered = array.filter(predicate);
        if (filtered.length === 0) return null;
        return filtered[0];
    }

}