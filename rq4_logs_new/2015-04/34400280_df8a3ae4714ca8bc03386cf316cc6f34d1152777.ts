declare module "atom" {
    /**
     * Atom global for dealing with packages, themes, menus, and the window.
     * 
     * An instance of this class is always available as the `atom` global. 
     *
     * file: src/atom.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/atom.coffee#L23
     */
    class Atom { 
        /**
         * A {CommandRegistry} instance 
         */
        commands: CommandRegistry;
        /**
         * A {Config} instance 
         */
        config: Config;
        /**
         * A {Clipboard} instance 
         */
        clipboard: Clipboard;
        /**
         * A {ContextMenuManager} instance 
         */
        contextMenu: ContextMenuManager;
        /**
         * A {MenuManager} instance 
         */
        menu: MenuManager;
        /**
         * A {KeymapManager} instance 
         */
        keymaps: KeymapManager;
        /**
         * A {TooltipManager} instance 
         */
        tooltips: TooltipManager;
        /**
         * A {Project} instance 
         */
        project: Project;
        /**
         * A {GrammarRegistry} instance 
         */
        grammars: GrammarRegistry;
        /**
         * A {PackageManager} instance 
         */
        packages: PackageManager;
        /**
         * A {ThemeManager} instance 
         */
        themes: ThemeManager;
        /**
         * A {StyleManager} instance 
         */
        styles: StyleManager;
        /**
         * A {DeserializerManager} instance 
         */
        deserializers: DeserializerManager;
        /**
         * A {ViewRegistry} instance 
         */
        views: ViewRegistry;
        /**
         * A {Workspace} instance 
         */
        workspace: Workspace;

        /**
         * Invoke the given callback whenever {::beep} is called.
         * @param {Function} {Function} to be called whenever {::beep} is called.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidBeep(callback: Function): Disposable;
        /**
         * Invoke the given callback when there is an unhandled error, but
         * before the devtools pop open
         * @param {Function} {Function} to be called whenever there is an unhandled error
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillThrowError(callback: Function): Disposable;
        /**
         * Invoke the given callback whenever there is an unhandled error.
         * @param {Function} {Function} to be called whenever there is an unhandled error
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidThrowError(callback: Function): Disposable;
        /**
         * Is the current window in development mode? 
         */
        inDevMode(): void;
        /**
         * Is the current window in safe mode? 
         */
        inSafeMode(): void;
        /**
         * Is the current window running specs? 
         */
        inSpecMode(): void;
        /**
         * Get the version of the Atom application.
         * @returns {string} Returns the version text {String}.
         */
        getVersion(): string;
        /**
         * Determine whether the current version is an official release. 
         */
        isReleasedVersion(): void;
        /**
         * Get the time taken to completely load the current window.
         * 
         * This time include things like loading and activating packages, creating
         * DOM elements for the editor, and reading the config.
         * @returns {number} Returns the {Number} of milliseconds taken to load the window or null
        if the window hasn't finished loading yet.
         */
        getWindowLoadTime(): number;
        /**
         * Open a new Atom window using the given options.
         * 
         * Calling this method without an options parameter will open a prompt to pick
         * a file/folder to open in the new window.
         * @param {Object} An {Object} with the following keys:
         */
        open(options: Object): void;
        /**
         * Prompt the user to select one or more folders.
         * @param {Function} A {Function} to call once the user has confirmed the selection.
         */
        pickFolder(callback: Function): void;
        /**
         * Close the current window. 
         */
        close(): void;
        /**
         * Get the size of current window.
         * @returns {Object} Returns an {Object} in the format `{width: 1000, height: 700}`
         */
        getSize(): Object;
        /**
         * Set the size of current window.
         * @param {number} The {Number} of pixels.
         * @param {number} The {Number} of pixels. 
         */
        setSize(width: number, height: number): void;
        /**
         * Get the position of current window.
         * @returns {Object} Returns an {Object} in the format `{x: 10, y: 20}`
         */
        getPosition(): Object;
        /**
         * Set the position of current window.
         * @param {number} The {Number} of pixels.
         * @param {number} The {Number} of pixels. 
         */
        setPosition(x: number, y: number): void;
        /**
         * Move current window to the center of the screen. 
         */
        center(): void;
        /**
         * Focus the current window. 
         */
        focus(): void;
        /**
         * Show the current window. 
         */
        show(): void;
        /**
         * Hide the current window. 
         */
        hide(): void;
        /**
         * Reload the current window. 
         */
        reload(): void;
        /**
         * @returns {boolean} Returns a {Boolean} true when the current window is maximized.
         */
        isMaximixed(): boolean;
        /**
         * Is the current window in full screen mode? 
         */
        isFullScreen(): void;
        /**
         * Set the full screen state of the current window. 
         */
        setFullScreen(): void;
        /**
         * Toggle the full screen state of the current window. 
         */
        toggleFullScreen(): void;
        /**
         * Visually and audibly trigger a beep. 
         */
        beep(): void;
        /**
         * A flexible way to open a dialog akin to an alert dialog.
         * @param {Object} An {Object} with the following keys:
         * @returns {number} Returns the chosen button index {Number} if the buttons option was an array.
         */
        confirm(options: Object): number;
        /**
         * Open the dev tools for the current window. 
         */
        openDevTools(): void;
        /**
         * Toggle the visibility of the dev tools for the current window. 
         */
        toggleDevTools(): void;
        /**
         * Execute code in dev tools. 
         */
        executeJavaScriptInDevTools(): void;
    }

    /**
     * Like {BufferedProcess}, but accepts a Node script as the command
     * to run.
     * 
     * This is necessary on Windows since it doesn't support shebang `#!` lines.
     *
     * file: src/buffered-node-process.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/buffered-node-process.coffee#L15
     */
    class BufferedNodeProcess extends BufferedProcess { 
        /**
         * Runs the given Node script by spawning a new child process.
         * @param {Object} An {Object} with the following keys:
         */
        constructor(options: Object);
    }

    /**
     * A wrapper which provides standard error/output line buffering for
     * Node's ChildProcess.
     *
     * file: src/buffered-process.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/buffered-process.coffee#L21
     */
    class BufferedProcess { 
        /**
         * Runs the given command by spawning a new child process.
         * @param {Object} An {Object} with the following keys:
         */
        constructor(options: Object);
        /**
         * Will call your callback when an error will be raised by the process.
         * Usually this is due to the command not being available or not on the PATH.
         * You can call `handle()` on the object passed to your callback to indicate
         * that you have handled this error.
         * @param {Function} {Function} callback
         * @returns {Disposable} Returns a {Disposable}
         */
        onWillThrowError(callback: Function): Disposable;
        /**
         * Terminate the process. 
         */
        kill(): void;
    }

    /**
     * Represents the clipboard used for copying and pasting in Atom.
     * 
     * An instance of this class is always available as the `atom.clipboard` global.
     *
     * file: src/clipboard.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/clipboard.coffee#L16
     */
    class Clipboard { 
        /**
         * Write the given text to the clipboard.
         * 
         * The metadata associated with the text is available by calling
         * {::readWithMetadata}.
         * @param {string} The {String} to store.
         * @param  The additional info to associate with the text. 
         */
        write(text: string, metadata: any): void;
        /**
         * Read the text from the clipboard.
         * @returns {string} Returns a {String}.
         */
        read(): string;
        /**
         * Read the text from the clipboard and return both the text and the
         * associated metadata.
         * @returns {Object} Returns an {Object} with the following keys:
        
        * `text` The {String} clipboard text.
        * `metadata` The metadata stored by an earlier call to {::write}.
         */
        readWithMetadata(): Object;
    }

    /**
     * A simple color class returned from {Config::get} when the value
     * at the key path is of type 'color'. 
     *
     * file: src/color.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/color.coffee#L7
     */
    class Color { 
        /**
         * Parse a {String} or {Object} into a {Color}.
         * @param {string} A {String} such as `'white'`, `#ff00ff`, or `'rgba(255, 15, 60, .75)'` or an {Object} with `red`, `green`, `blue`, and `alpha` properties.
         * @returns {Color} Returns a {Color} or `null` if it cannot be parsed.
         */
        static parse(value: string): Color;

        /**
         * @returns {string} Returns a {String} in the form `'#abcdef'`.
         */
        toHexString(): string;
        /**
         * @returns {string} Returns a {String} in the form `'rgba(25, 50, 75, .9)'`.
         */
        toRGBAString(): string;
    }

    /**
     * Associates listener functions with commands in a
     * context-sensitive way using CSS selectors. You can access a global instance of
     * this class via `atom.commands`, and commands registered there will be
     * presented in the command palette.
     * 
     * The global command registry facilitates a style of event handling known as
     * *event delegation* that was popularized by jQuery. Atom commands are expressed
     * as custom DOM events that can be invoked on the currently focused element via
     * a key binding or manually via the command palette. Rather than binding
     * listeners for command events directly to DOM nodes, you instead register
     * command event listeners globally on `atom.commands` and constrain them to
     * specific kinds of elements with CSS selectors.
     * 
     * As the event bubbles upward through the DOM, all registered event listeners
     * with matching selectors are invoked in order of specificity. In the event of a
     * specificity tie, the most recently registered listener is invoked first. This
     * mirrors the "cascade" semantics of CSS. Event listeners are invoked in the
     * context of the current DOM node, meaning `this` always points at
     * `event.currentTarget`. As is normally the case with DOM events,
     * `stopPropagation` and `stopImmediatePropagation` can be used to terminate the
     * bubbling process and prevent invocation of additional listeners.
     * 
     * ## Example
     * 
     * Here is a command that inserts the current date in an editor:
     * 
     * ```coffee
     * atom.commands.add 'atom-text-editor',
     *   'user:insert-date': (event) ->
     *     editor = @getModel()
     *     editor.insertText(new Date().toLocaleString())
     * ```
     *
     * file: src/command-registry.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/command-registry.coffee#L41
     */
    class CommandRegistry { 
        /**
         * Add one or more command listeners associated with a selector.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        added command handler(s).
         */
        add(): Disposable;
        /**
         * Find all registered commands matching a query.
         * @param {Object} An {Object} containing one or more of the following keys:
         * @returns {any[]} Returns an {Array} of {Object}s containing the following keys:
        
        * `name` The name of the command. For example, `user:insert-date`.
        * `displayName` The display name of the command. For example,
          `User: Insert Date`.
        * `jQuery` Present if the command was registered with the legacy
          `$::command` method.
         */
        findCommands(params: Object): any[];
        /**
         * Simulate the dispatch of a command on a DOM node.
         * 
         * This can be useful for testing when you want to simulate the invocation of a
         * command on a detached DOM node. Otherwise, the DOM node in question needs to
         * be attached to the document so the event bubbles up to the root node to be
         * processed.
         * @param  The DOM node at which to start bubbling the command event.
         * @param {string} {String} indicating the name of the command to dispatch. 
         */
        dispatch(target: any, commandName: string): void;
    }

    /**
     * An object that aggregates multiple {Disposable} instances together
     * into a single disposable, so they can all be disposed as a group.
     * 
     * These are very useful when subscribing to multiple events.
     *
     * file: src/composite-disposable.coffee
     * srcUrl: https://github.com/atom/event-kit/blob/v1.1.0/src/composite-disposable.coffee#L22
     */
    class CompositeDisposable { 
        /**
         * Construct an instance, optionally with one or more disposables 
         */
        constructor();
        /**
         * Dispose all disposables added to this composite disposable.
         * 
         * If this object has already been disposed, this method has no effect. 
         */
        dispose(): void;
        /**
         * Add a disposable to be disposed when the composite is disposed.
         * 
         * If this object has already been disposed, this method has no effect.
         * @param {Disposable} {Disposable} instance or any object with a `.dispose()` method. 
         */
        add(disposable: Disposable): void;
        /**
         * Remove a previously added disposable.
         * @param {Disposable} {Disposable} instance or any object with a `.dispose()` method. 
         */
        remove(disposable: Disposable): void;
        /**
         * Clear all disposables. They will not be disposed by the next call
         * to dispose. 
         */
        clear(): void;
    }

    /**
     * Used to access all of Atom's configuration details.
     * 
     * An instance of this class is always available as the `atom.config` global.
     * 
     * ## Getting and setting config settings.
     * 
     * ```coffee
     * # Note that with no value set, ::get returns the setting's default value.
     * atom.config.get('my-package.myKey') # -> 'defaultValue'
     * 
     * atom.config.set('my-package.myKey', 'value')
     * atom.config.get('my-package.myKey') # -> 'value'
     * ```
     * 
     * You may want to watch for changes. Use {::observe} to catch changes to the setting.
     * 
     * ```coffee
     * atom.config.set('my-package.myKey', 'value')
     * atom.config.observe 'my-package.myKey', (newValue) ->
     *   # `observe` calls immediately and every time the value is changed
     *   console.log 'My configuration changed:', newValue
     * ```
     * 
     * If you want a notification only when the value changes, use {::onDidChange}.
     * 
     * ```coffee
     * atom.config.onDidChange 'my-package.myKey', ({newValue, oldValue}) ->
     *   console.log 'My configuration changed:', newValue, oldValue
     * ```
     * 
     * ### Value Coercion
     * 
     * Config settings each have a type specified by way of a
     * [schema](json-schema.org). For example we might an integer setting that only
     * allows integers greater than `0`:
     * 
     * ```coffee
     * # When no value has been set, `::get` returns the setting's default value
     * atom.config.get('my-package.anInt') # -> 12
     * 
     * # The string will be coerced to the integer 123
     * atom.config.set('my-package.anInt', '123')
     * atom.config.get('my-package.anInt') # -> 123
     * 
     * # The string will be coerced to an integer, but it must be greater than 0, so is set to 1
     * atom.config.set('my-package.anInt', '-20')
     * atom.config.get('my-package.anInt') # -> 1
     * ```
     * 
     * ## Defining settings for your package
     * 
     * Define a schema under a `config` key in your package main.
     * 
     * ```coffee
     * module.exports =
     *   # Your config schema
     *   config:
     *     someInt:
     *       type: 'integer'
     *       default: 23
     *       minimum: 1
     * 
     *   activate: (state) -> # ...
     *   # ...
     * ```
     * 
     * See [package docs](https://atom.io/docs/latest/hacking-atom-package-word-count) for
     * more info.
     * 
     * ## Config Schemas
     * 
     * We use [json schema](http://json-schema.org) which allows you to define your value's
     * default, the type it should be, etc. A simple example:
     * 
     * ```coffee
     * # We want to provide an `enableThing`, and a `thingVolume`
     * config:
     *   enableThing:
     *     type: 'boolean'
     *     default: false
     *   thingVolume:
     *     type: 'integer'
     *     default: 5
     *     minimum: 1
     *     maximum: 11
     * ```
     * 
     * The type keyword allows for type coercion and validation. If a `thingVolume` is
     * set to a string `'10'`, it will be coerced into an integer.
     * 
     * ```coffee
     * atom.config.set('my-package.thingVolume', '10')
     * atom.config.get('my-package.thingVolume') # -> 10
     * 
     * # It respects the min / max
     * atom.config.set('my-package.thingVolume', '400')
     * atom.config.get('my-package.thingVolume') # -> 11
     * 
     * # If it cannot be coerced, the value will not be set
     * atom.config.set('my-package.thingVolume', 'cats')
     * atom.config.get('my-package.thingVolume') # -> 11
     * ```
     * 
     * ### Supported Types
     * 
     * The `type` keyword can be a string with any one of the following. You can also
     * chain them by specifying multiple in an an array. For example
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: ['boolean', 'integer']
     *     default: 5
     * 
     * # Then
     * atom.config.set('my-package.someSetting', 'true')
     * atom.config.get('my-package.someSetting') # -> true
     * 
     * atom.config.set('my-package.someSetting', '12')
     * atom.config.get('my-package.someSetting') # -> 12
     * ```
     * 
     * #### string
     * 
     * Values must be a string.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'string'
     *     default: 'hello'
     * ```
     * 
     * #### integer
     * 
     * Values will be coerced into integer. Supports the (optional) `minimum` and
     * `maximum` keys.
     * 
     * ```coffee
     *   config:
     *     someSetting:
     *       type: 'integer'
     *       default: 5
     *       minimum: 1
     *       maximum: 11
     * ```
     * 
     * #### number
     * 
     * Values will be coerced into a number, including real numbers. Supports the
     * (optional) `minimum` and `maximum` keys.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'number'
     *     default: 5.3
     *     minimum: 1.5
     *     maximum: 11.5
     * ```
     * 
     * #### boolean
     * 
     * Values will be coerced into a Boolean. `'true'` and `'false'` will be coerced into
     * a boolean. Numbers, arrays, objects, and anything else will not be coerced.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'boolean'
     *     default: false
     * ```
     * 
     * #### array
     * 
     * Value must be an Array. The types of the values can be specified by a
     * subschema in the `items` key.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'array'
     *     default: [1, 2, 3]
     *     items:
     *       type: 'integer'
     *       minimum: 1.5
     *       maximum: 11.5
     * ```
     * 
     * #### object
     * 
     * Value must be an object. This allows you to nest config options. Sub options
     * must be under a `properties key`
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'object'
     *     properties:
     *       myChildIntOption:
     *         type: 'integer'
     *         minimum: 1.5
     *         maximum: 11.5
     * ```
     * 
     * #### color
     * 
     * Values will be coerced into a {Color} with `red`, `green`, `blue`, and `alpha`
     * properties that all have numeric values. `red`, `green`, `blue` will be in
     * the range 0 to 255 and `value` will be in the range 0 to 1. Values can be any
     * valid CSS color format such as `#abc`, `#abcdef`, `white`,
     * `rgb(50, 100, 150)`, and `rgba(25, 75, 125, .75)`.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'color'
     *     default: 'white'
     * ```
     * 
     * ### Other Supported Keys
     * 
     * #### enum
     * 
     * All types support an `enum` key. The enum key lets you specify all values
     * that the config setting can possibly be. `enum` _must_ be an array of values
     * of your specified type. Schema:
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     type: 'integer'
     *     default: 4
     *     enum: [2, 4, 6, 8]
     * ```
     * 
     * Usage:
     * 
     * ```coffee
     * atom.config.set('my-package.someSetting', '2')
     * atom.config.get('my-package.someSetting') # -> 2
     * 
     * # will not set values outside of the enum values
     * atom.config.set('my-package.someSetting', '3')
     * atom.config.get('my-package.someSetting') # -> 2
     * 
     * # If it cannot be coerced, the value will not be set
     * atom.config.set('my-package.someSetting', '4')
     * atom.config.get('my-package.someSetting') # -> 4
     * ```
     * 
     * #### title and description
     * 
     * The settings view will use the `title` and `description` keys to display your
     * config setting in a readable way. By default the settings view humanizes your
     * config key, so `someSetting` becomes `Some Setting`. In some cases, this is
     * confusing for users, and a more descriptive title is useful.
     * 
     * Descriptions will be displayed below the title in the settings view.
     * 
     * ```coffee
     * config:
     *   someSetting:
     *     title: 'Setting Magnitude'
     *     description: 'This will affect the blah and the other blah'
     *     type: 'integer'
     *     default: 4
     * ```
     * 
     * __Note__: You should strive to be so clear in your naming of the setting that
     * you do not need to specify a title or description!
     * 
     * ## Best practices
     * 
     * * Don't depend on (or write to) configuration keys outside of your keypath.
     *
     * file: src/config.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/config.coffee#L291
     */
    class Config { 
        /**
         * Add a listener for changes to a given key path. This is different
         * than {::onDidChange} in that it will immediately call your callback with the
         * current value of the config entry.
         * 
         * ### Examples
         * 
         * You might want to be notified when the themes change. We'll watch
         * `core.themes` for changes
         * 
         * ```coffee
         * atom.config.observe 'core.themes', (value) ->
         *   # do stuff with value
         * ```
         * @param {string} {String} name of the key to observe
         * @param {Object} {Object}
         * @param {Function} {Function} to call when the value of the key changes.
         * @returns {Disposable} Returns a {Disposable} with the following keys on which you can call
        `.dispose()` to unsubscribe.
         */
        observe(keyPath: string, options: Object, callback: Function): Disposable;
        /**
         * Add a listener for changes to a given key path. If `keyPath` is
         * not specified, your callback will be called on changes to any key.
         * @param {string} {String} name of the key to observe. Must be specified if `scopeDescriptor` is specified.
         * @param {Object} {Object}
         * @param {Function} {Function} to call when the value of the key changes.
         * @returns {Disposable} Returns a {Disposable} with the following keys on which you can call
        `.dispose()` to unsubscribe.
         */
        onDidChange(keyPath?: string, optional?: Object, callback?: Function): Disposable;
        /**
         * Retrieves the setting for the given key.
         * 
         * ### Examples
         * 
         * You might want to know what themes are enabled, so check `core.themes`
         * 
         * ```coffee
         * atom.config.get('core.themes')
         * ```
         * 
         * With scope descriptors you can get settings within a specific editor
         * scope. For example, you might want to know `editor.tabLength` for ruby
         * files.
         * 
         * ```coffee
         * atom.config.get('editor.tabLength', scope: ['source.ruby']) # => 2
         * ```
         * 
         * This setting in ruby files might be different than the global tabLength setting
         * 
         * ```coffee
         * atom.config.get('editor.tabLength') # => 4
         * atom.config.get('editor.tabLength', scope: ['source.ruby']) # => 2
         * ```
         * 
         * You can get the language scope descriptor via
         * {TextEditor::getRootScopeDescriptor}. This will get the setting specifically
         * for the editor's language.
         * 
         * ```coffee
         * atom.config.get('editor.tabLength', scope: @editor.getRootScopeDescriptor()) # => 2
         * ```
         * 
         * Additionally, you can get the setting at the specific cursor position.
         * 
         * ```coffee
         * scopeDescriptor = @editor.getLastCursor().getScopeDescriptor()
         * atom.config.get('editor.tabLength', scope: scopeDescriptor) # => 2
         * ```
         * @param {string} The {String} name of the key to retrieve.
         * @param {Object} {Object}
         * @returns  Returns the value from Atom's default settings, the user's configuration
        file in the type specified by the configuration schema.
         */
        get(keyPath: string, options?: Object): any;
        /**
         * Get all of the values for the given key-path, along with their
         * associated scope selector.
         * @param {string} The {String} name of the key to retrieve
         * @param {Object} {Object} see the `options` argument to {::get}
         * @returns {any[]} Returns an {Array} of {Object}s with the following keys:
        
        * `scopeDescriptor` The {ScopeDescriptor} with which the value is associated
        * `value` The value for the key-path
         */
        getAll(keyPath: string, options?: Object): any[];
        /**
         * Sets the value for a configuration setting.
         * 
         * This value is stored in Atom's internal configuration file.
         * 
         * ### Examples
         * 
         * You might want to change the themes programmatically:
         * 
         * ```coffee
         * atom.config.set('core.themes', ['atom-light-ui', 'atom-light-syntax'])
         * ```
         * 
         * You can also set scoped settings. For example, you might want change the
         * `editor.tabLength` only for ruby files.
         * 
         * ```coffee
         * atom.config.get('editor.tabLength') # => 4
         * atom.config.get('editor.tabLength', scope: ['source.ruby']) # => 4
         * atom.config.get('editor.tabLength', scope: ['source.js']) # => 4
         * 
         * # Set ruby to 2
         * atom.config.set('editor.tabLength', 2, scopeSelector: 'source.ruby') # => true
         * 
         * # Notice it's only set to 2 in the case of ruby
         * atom.config.get('editor.tabLength') # => 4
         * atom.config.get('editor.tabLength', scope: ['source.ruby']) # => 2
         * atom.config.get('editor.tabLength', scope: ['source.js']) # => 4
         * ```
         * @param {string} The {String} name of the key.
         * @param  The value of the setting. Passing `undefined` will revert the setting to the default value.
         * @param {Object} {Object}
         * @returns {boolean} Returns a {Boolean}
        
        * `true` if the value was set.
        * `false` if the value was not able to be coerced to the type specified in the setting's schema.
         */
        set(keyPath: string, value: any, options?: Object): boolean;
        /**
         * Restore the setting at `keyPath` to its default value.
         * @param {string} The {String} name of the key.
         * @param {Object} {Object}
         */
        unset(keyPath: string, options?: Object): void;
        /**
         * Get an {Array} of all of the `source` {String}s with which
         * settings have been added via {::set}. 
         */
        getSources(): void;
        /**
         * Retrieve the schema for a specific key path. The schema will tell
         * you what type the keyPath expects, and other metadata about the config
         * option.
         * @param {string} The {String} name of the key.
         * @returns {Object} Returns an {Object} eg. `{type: 'integer', default: 23, minimum: 1}`.
         * @returns  Returns `null` when the keyPath has no schema specified.
         */
        getSchema(keyPath: string): Object;
        /**
         * Get the {String} path to the config file being used. 
         */
        getUserConfigPath(): void;
        /**
         * Suppress calls to handler functions registered with {::onDidChange}
         * and {::observe} for the duration of `callback`. After `callback` executes,
         * handlers will be called once if the value for their key-path has changed.
         * @param {Function} {Function} to execute while suppressing calls to handlers. 
         */
        transact(callback: Function): void;
    }

    /**
     * Provides a registry for commands that you'd like to appear in the
     * context menu.
     * 
     * An instance of this class is always available as the `atom.contextMenu`
     * global.
     * 
     * ## Context Menu CSON Format
     * 
     * ```coffee
     * 'atom-workspace': [{label: 'Help', command: 'application:open-documentation'}]
     * 'atom-text-editor': [{
     *   label: 'History',
     *   submenu: [
     *     {label: 'Undo', command:'core:undo'}
     *     {label: 'Redo', command:'core:redo'}
     *   ]
     * }]
     * ```
     * 
     * In your package's menu `.cson` file you need to specify it under a
     * `context-menu` key:
     * 
     * ```coffee
     * 'context-menu':
     *   'atom-workspace': [{label: 'Help', command: 'application:open-documentation'}]
     *   ...
     * ```
     * 
     * The format for use in {::add} is the same minus the `context-menu` key. See
     * {::add} for more information. 
     *
     * file: src/context-menu-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/context-menu-manager.coffee#L43
     */
    class ContextMenuManager { 
        /**
         * Add context menu items scoped by CSS selectors.
         * @param {Object} An {Object} whose keys are CSS selectors and whose values are {Array}s of item {Object}s containing the following keys:
         */
        add(itemsBySelector: Object): void;
    }

    /**
     * The `Cursor` class represents the little blinking line identifying
     * where text can be inserted.
     * 
     * Cursors belong to {TextEditor}s and have some metadata attached in the form
     * of a {Marker}. 
     *
     * file: src/cursor.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/cursor.coffee#L13
     */
    class Cursor { 
        /**
         * Calls your `callback` when the cursor has been moved.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangePosition(callback: Function): Disposable;
        /**
         * Calls your `callback` when the cursor is destroyed
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * Calls your `callback` when the cursor's visibility has changed
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeVisibility(callback: Function): Disposable;
        /**
         * Moves a cursor to a given screen position.
         * @param {any[]} {Array} of two numbers: the screen row, and the screen column.
         * @param {Object} {Object} with the following keys:
         */
        setScreenPosition(screenPosition: any[], options?: Object): void;
        /**
         * @returns  Returns the screen position of the cursor as an Array.
         */
        getScreenPosition(): any;
        /**
         * Moves a cursor to a given buffer position.
         * @param {any[]} {Array} of two numbers: the buffer row, and the buffer column.
         * @param {Object} {Object} with the following keys:
         */
        setBufferPosition(bufferPosition: any[], options?: Object): void;
        /**
         * @returns  Returns the current buffer position as an Array.
         */
        getBufferPosition(): any;
        /**
         * @returns  Returns the cursor's current screen row.
         */
        getScreenRow(): any;
        /**
         * @returns  Returns the cursor's current screen column.
         */
        getScreenColumn(): any;
        /**
         * Retrieves the cursor's current buffer row. 
         */
        getBufferRow(): void;
        /**
         * @returns  Returns the cursor's current buffer column.
         */
        getBufferColumn(): any;
        /**
         * @returns  Returns the cursor's current buffer row of text excluding its line
        ending.
         */
        getCurrentBufferLine(): any;
        /**
         * @returns  Returns whether the cursor is at the start of a line.
         */
        isAtBeginningOfLine(): any;
        /**
         * @returns  Returns whether the cursor is on the line return character.
         */
        isAtEndOfLine(): any;
        /**
         * @returns {Marker} Returns the underlying {Marker} for the cursor.
        Useful with overlay {Decoration}s.
         */
        getMarker(): Marker;
        /**
         * Identifies if the cursor is surrounded by whitespace.
         * 
         * "Surrounded" here means that the character directly before and after the
         * cursor are both whitespace.
         * @returns {boolean} Returns a {Boolean}.
         */
        isSurroundedByWhitespace(): boolean;
        /**
         * 
         * 
         * This method returns false if the character before or after the cursor is
         * whitespace.
         * @returns  Returns whether the cursor is currently between a word and non-word
        character. The non-word characters are defined by the
        `editor.nonWordCharacters` config value.
         * @returns  Returns a Boolean.
         */
        isBetweenWordAndNonWord(): any;
        /**
         * @param {Object} {Object}
         * @returns  Returns whether this cursor is between a word's start and end.
         * @returns {boolean} Returns a {Boolean}
         */
        isInsideWord(options?: Object): boolean;
        /**
         * @returns  Returns the indentation level of the current line.
         */
        getIndentLevel(): any;
        /**
         * Retrieves the scope descriptor for the cursor's current position.
         * @returns {ScopeDescriptor} Returns a {ScopeDescriptor}
         */
        getScopeDescriptor(): ScopeDescriptor;
        /**
         * @returns  Returns true if this cursor has no non-whitespace characters before
        its current position.
         */
        hasPrecedingCharactersOnLine(): any;
        /**
         * Identifies if this cursor is the last in the {TextEditor}.
         * 
         * "Last" is defined as the most recently added cursor.
         * @returns {boolean} Returns a {Boolean}.
         */
        isLastCursor(): boolean;
        /**
         * Moves the cursor up one screen row.
         * @param {number} {Number} number of rows to move (default: 1)
         * @param {Object} {Object} with the following keys:
         */
        moveUp(rowCount?: number, options?: Object): void;
        /**
         * Moves the cursor down one screen row.
         * @param {number} {Number} number of rows to move (default: 1)
         * @param {Object} {Object} with the following keys:
         */
        moveDown(rowCount?: number, options?: Object): void;
        /**
         * Moves the cursor left one screen column.
         * @param {number} {Number} number of columns to move (default: 1)
         * @param {Object} {Object} with the following keys:
         */
        moveLeft(columnCount?: number, options?: Object): void;
        /**
         * Moves the cursor right one screen column.
         * @param {number} {Number} number of columns to move (default: 1)
         * @param {Object} {Object} with the following keys:
         */
        moveRight(columnCount?: number, options?: Object): void;
        /**
         * Moves the cursor to the top of the buffer. 
         */
        moveToTop(): void;
        /**
         * Moves the cursor to the bottom of the buffer. 
         */
        moveToBottom(): void;
        /**
         * Moves the cursor to the beginning of the line. 
         */
        moveToBeginningOfScreenLine(): void;
        /**
         * Moves the cursor to the beginning of the buffer line. 
         */
        moveToBeginningOfLine(): void;
        /**
         * Moves the cursor to the beginning of the first character in the
         * line. 
         */
        moveToFirstCharacterOfLine(): void;
        /**
         * Moves the cursor to the end of the line. 
         */
        moveToEndOfScreenLine(): void;
        /**
         * Moves the cursor to the end of the buffer line. 
         */
        moveToEndOfLine(): void;
        /**
         * Moves the cursor to the beginning of the word. 
         */
        moveToBeginningOfWord(): void;
        /**
         * Moves the cursor to the end of the word. 
         */
        moveToEndOfWord(): void;
        /**
         * Moves the cursor to the beginning of the next word. 
         */
        moveToBeginningOfNextWord(): void;
        /**
         * Moves the cursor to the previous word boundary. 
         */
        moveToPreviousWordBoundary(): void;
        /**
         * Moves the cursor to the next word boundary. 
         */
        moveToNextWordBoundary(): void;
        /**
         * Moves the cursor to the beginning of the buffer line, skipping all
         * whitespace. 
         */
        skipLeadingWhitespace(): void;
        /**
         * Moves the cursor to the beginning of the next paragraph 
         */
        moveToBeginningOfNextParagraph(): void;
        /**
         * Moves the cursor to the beginning of the previous paragraph 
         */
        moveToBeginningOfPreviousParagraph(): void;
        /**
         * @param {Object} {Object} with the following keys:
         * @returns  Returns buffer position of previous word boundary. It might be on
        the current word, or the previous word.
         */
        getPreviousWordBoundaryBufferPosition(options?: Object): any;
        /**
         * @param {Object} {Object} with the following keys:
         * @returns  Returns buffer position of the next word boundary. It might be on
        the current word, or the previous word.
         */
        getNextWordBoundaryBufferPosition(options?: Object): any;
        /**
         * Retrieves the buffer position of where the current word starts.
         * @param {Object} An {Object} with the following keys:
         * @returns {Range} Returns a {Range}.
         */
        getBeginningOfCurrentWordBufferPosition(options?: Object): Range;
        /**
         * Retrieves the buffer position of where the current word ends.
         * @param {Object} {Object} with the following keys:
         * @returns {Range} Returns a {Range}.
         */
        getEndOfCurrentWordBufferPosition(options?: Object): Range;
        /**
         * Retrieves the buffer position of where the next word starts.
         * @param {Object} {Object}
         * @returns {Range} Returns a {Range}
         */
        getBeginningOfNextWordBufferPosition(options?: Object): Range;
        /**
         * @param {Object} {Object}
         * @returns  Returns the buffer Range occupied by the word located under the cursor.
         */
        getCurrentWordBufferRange(options?: Object): any;
        /**
         * @param {Object} {Object}
         * @returns  Returns the buffer Range for the current line.
         */
        getCurrentLineBufferRange(options?: Object): any;
        /**
         * Retrieves the range for the current paragraph.
         * 
         * A paragraph is defined as a block of text surrounded by empty lines.
         * @returns {Range} Returns a {Range}.
         */
        getCurrentParagraphBufferRange(): Range;
        /**
         * @returns  Returns the characters preceding the cursor in the current word.
         */
        getCurrentWordPrefix(): any;
        /**
         * Sets whether the cursor is visible. 
         */
        setVisible(): void;
        /**
         * @returns  Returns the visibility of the cursor.
         */
        isVisible(): any;
        /**
         * Compare this cursor's buffer position to another cursor's buffer position.
         * 
         * See {Point::compare} for more details.
         * @param {Cursor} {Cursor} to compare against 
         */
        compare(otherCursor: Cursor): void;
        /**
         * Prevents this cursor from causing scrolling. 
         */
        clearAutoscroll(): void;
        /**
         * Deselects the current selection. 
         */
        clearSelection(): void;
        /**
         * Get the RegExp used by the cursor to determine what a "word" is.
         * @param {Object} {Object} with the following keys:
         * @returns {RegExp} Returns a {RegExp}.
         */
        wordRegExp(options?: Object): RegExp;
    }

    /**
     * Represents a decoration that follows a {Marker}. A decoration is
     * basically a visual representation of a marker. It allows you to add CSS
     * classes to line numbers in the gutter, lines, and add selection-line regions
     * around marked ranges of text.
     * 
     * {Decoration} objects are not meant to be created directly, but created with
     * {TextEditor::decorateMarker}. eg.
     * 
     * ```coffee
     * range = editor.getSelectedBufferRange() # any range you like
     * marker = editor.markBufferRange(range)
     * decoration = editor.decorateMarker(marker, {type: 'line', class: 'my-line-class'})
     * ```
     * 
     * Best practice for destroying the decoration is by destroying the {Marker}.
     * 
     * ```coffee
     * marker.destroy()
     * ```
     * 
     * You should only use {Decoration::destroy} when you still need or do not own
     * the marker. 
     *
     * file: src/decoration.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/decoration.coffee#L31
     */
    class Decoration { 
        /**
         * Destroy this marker.
         * 
         * If you own the marker, you should use {Marker::destroy} which will destroy
         * this decoration. 
         */
        destroy(): void;
        /**
         * When the {Decoration} is updated via {Decoration::update}.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeProperties(callback: Function): Disposable;
        /**
         * Invoke the given callback when the {Decoration} is destroyed
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * An id unique across all {Decoration} objects 
         */
        getId(): void;
        /**
         * @returns {Decoration} Returns the marker associated with this {Decoration}
         */
        getMarker(): Decoration;
        /**
         * @returns {Decoration} Returns the {Decoration}'s properties.
         */
        getProperties(): Decoration;
        /**
         * Update the marker with new Properties. Allows you to change the decoration's class.
         * @param {Object} {Object} eg. `{type: 'line-number', class: 'my-new-class'}` 
         */
        setProperties(newProperties: Object): void;
    }

    /**
     * Manages the deserializers used for serialized state
     * 
     * An instance of this class is always available as the `atom.deserializers`
     * global.
     *
     * file: src/deserializer-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/deserializer-manager.coffee#L24
     */
    class DeserializerManager { 
        /**
         * Register the given class(es) as deserializers.
         * @param  One or more deserializers to register. A deserializer can be any object with a `.name` property and a `.deserialize()` method. A common approach is to register a *constructor* as the deserializer for its instances by adding a `.deserialize()` class method. 
         */
        add(deserializers: any): void;
        /**
         * Deserialize the state and params.
         * @param {Object} The state {Object} to deserialize.
         * @param {Object} The params {Object} to pass as the second arguments to the deserialize method of the deserializer. 
         */
        deserialize(state: Object, params: Object): void;
    }

    /**
     * Represents a directory on disk that can be watched for changes. 
     *
     * file: src/directory.coffee
     * srcUrl: https://github.com/atom/node-pathwatcher/blob/v4.4.0/src/directory.coffee#L14
     */
    class Directory { 
        /**
         * Configures a new Directory instance, no files are accessed.
         * @param {string} A {String} containing the absolute path to the directory
         * @param {boolean} A {Boolean} indicating if the path is a symlink. (default: false) 
         */
        constructor(directoryPath: string, symlink?: boolean);
        /**
         * Creates the directory on disk that corresponds to `::getPath()` if
         * no such directory already exists.
         * @param {number} Optional {Number} that defaults to `0777`. Returns a {Promise} that resolves once the directory is created on disk. It resolves to a boolean value that is true if the directory was created or false if it already existed. 
         */
        create(mode: number): void;
        /**
         * Invoke the given callback when the directory's contents change.
         * @param {Function} {Function} to be called when the directory's contents change.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChange(callback: Function): Disposable;
        /**
         * @returns {boolean} Returns a {Boolean}, always false.
         */
        isFile(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean}, always true.
         */
        isDirectory(): boolean;
        /**
         * @returns {boolean} Returns a promise that resolves to a {Boolean}, true if the
        directory exists, false otherwise.
         */
        exists(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean}, true if the directory exists, false otherwise.
         */
        existsSync(): boolean;
        /**
         * Return a {Boolean}, true if this {Directory} is the root directory
         * of the filesystem, or false if it isn't. 
         */
        isRoot(): void;
        /**
         * 
         * 
         * This may include unfollowed symlinks or relative directory entries. Or it
         * may be fully resolved, it depends on what you give it. 
         * @returns {string} Returns the directory's {String} path.
         */
        getPath(): string;
        /**
         * 
         * 
         * All relative directory entries are removed and symlinks are resolved to
         * their final destination. 
         * @returns {string} Returns this directory's completely resolved {String} path.
         */
        getRealPathSync(): string;
        /**
         * @returns {string} Returns the {String} basename of the directory.
         */
        getBaseName(): string;
        /**
         * @returns {string} Returns the relative {String} path to the given path from this
        directory.
         */
        relativize(): string;
        /**
         * Traverse to the parent directory.
         * @returns {Directory} Returns a {Directory}.
         */
        getParent(): Directory;
        /**
         * Traverse within this Directory to a child File. This method doesn't
         * actually check to see if the File exists, it just creates the File object.
         * @param {string} The {String} name of a File within this Directory.
         * @returns {File} Returns a {File}.
         */
        getFile(filename: string): File;
        /**
         * Traverse within this a Directory to a child Directory. This method
         * doesn't actually check to see if the Directory exists, it just creates the
         * Directory object.
         * @param {string} The {String} name of the child Directory.
         * @returns {Directory} Returns a {Directory}.
         */
        getSubdirectory(dirname: string): Directory;
        /**
         * Reads file entries in this directory from disk synchronously.
         * @returns {any[]} Returns an {Array} of {File} and {Directory} objects.
         */
        getEntriesSync(): any[];
        /**
         * Reads file entries in this directory from disk asynchronously.
         * @param {Function} A {Function} to call with the following arguments:
         */
        getEntries(callback: Function): void;
        /**
         * @returns  Returns whether the given path (real or symbolic) is inside this
        directory. This method does not actually check if the path exists, it just
        checks if the path is under this directory.
         */
        contains(): any;
    }

    /**
     * A handle to a resource that can be disposed. For example,
     * {Emitter::on} returns disposables representing subscriptions. 
     *
     * file: src/disposable.coffee
     * srcUrl: https://github.com/atom/event-kit/blob/v1.1.0/src/disposable.coffee#L6
     */
    class Disposable { 
        /**
         * Construct a Disposable
         * @param  An action to perform when {::dispose} is called for the first time. 
         */
        constructor(disposalAction: any);
        /**
         * Perform the disposal action, indicating that the resource associated
         * with this disposable is no longer needed.
         * 
         * You can call this method more than once, but the disposal action will only
         * be performed the first time. 
         */
        dispose(): void;
    }

    /**
     * Utility class to be used when implementing event-based APIs that
     * allows for handlers registered via `::on` to be invoked with calls to
     * `::emit`. Instances of this class are intended to be used internally by
     * classes that expose an event-based API.
     * 
     * For example:
     * 
     * ```coffee
     * class User
     *   constructor: ->
     *     @emitter = new Emitter
     * 
     *   onDidChangeName: (callback) ->
     *     @emitter.on 'did-change-name', callback
     * 
     *   setName: (name) ->
     *     if name isnt @name
     *       @name = name
     *       @emitter.emit 'did-change-name', name
     *     @name
     * ```
     *
     * file: src/emitter.coffee
     * srcUrl: https://github.com/atom/event-kit/blob/v1.1.0/src/emitter.coffee#L25
     */
    class Emitter { 
        /**
         * Construct an emitter.
         * 
         * ```coffee
         * @emitter = new Emitter()
         * ```
         */
        constructor();
        /**
         * Unsubscribe all handlers. 
         */
        dispose(): void;
        /**
         * Register the given handler function to be invoked whenever events by
         * the given name are emitted via {::emit}.
         * @param {string} {String} naming the event that you want to invoke the handler when emitted.
         * @param {Function} {Function} to invoke when {::emit} is called with the given event name.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        on(eventName: string, handler: Function): Disposable;
        /**
         * Register the given handler function to be invoked *before* all
         * other handlers existing at the time of subscription whenever events by the
         * given name are emitted via {::emit}.
         * 
         * Use this method when you need to be the first to handle a given event. This
         * could be required when a data structure in a parent object needs to be
         * updated before third-party event handlers registered on a child object via a
         * public API are invoked. Your handler could itself be preempted via
         * subsequent calls to this method, but this can be controlled by keeping
         * methods based on `::preempt` private.
         * @param {string} {String} naming the event that you want to invoke the handler when emitted.
         * @param {Function} {Function} to invoke when {::emit} is called with the given event name.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        preempt(eventName: string, handler: Function): Disposable;
        /**
         * Invoke handlers registered via {::on} for the given event name.
         * @param  The name of the event to emit. Handlers registered with {::on} for the same name will be invoked.
         * @param  Callbacks will be invoked with this value as an argument. 
         */
        emit(eventName: any, value: any): void;
    }

    /**
     * Represents an individual file that can be watched, read from, and
     * written to. 
     *
     * file: src/file.coffee
     * srcUrl: https://github.com/atom/node-pathwatcher/blob/v4.4.0/src/file.coffee#L19
     */
    class File { 
        /**
         * Configures a new File instance, no files are accessed.
         * @param {string} A {String} containing the absolute path to the file
         * @param {boolean} A {Boolean} indicating if the path is a symlink (default: false). 
         */
        constructor(filePath: string, symlink: boolean);
        /**
         * Creates the file on disk that corresponds to `::getPath()` if no
         * such file already exists.
         * @returns {Promise<any>} Returns a {Promise} that resolves once the file is created on disk. It
        resolves to a boolean value that is true if the file was created or false if
        it already existed.
         */
        create(): Promise<any>;
        /**
         * Invoke the given callback when the file's contents change.
         * @param {Function} {Function} to be called when the file's contents change.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChange(callback: Function): Disposable;
        /**
         * Invoke the given callback when the file's path changes.
         * @param {Function} {Function} to be called when the file's path changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidRename(callback: Function): Disposable;
        /**
         * Invoke the given callback when the file is deleted.
         * @param {Function} {Function} to be called when the file is deleted.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDelete(callback: Function): Disposable;
        /**
         * Invoke the given callback when there is an error with the watch.
         * When your callback has been invoked, the file will have unsubscribed from
         * the file watches.
         * @param {Function} {Function} callback
         */
        onWillThrowWatchError(callback: Function): void;
        /**
         * @returns {boolean} Returns a {Boolean}, always true.
         */
        isFile(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean}, always false.
         */
        isDirectory(): boolean;
        /**
         * @returns {boolean} Returns a promise that resolves to a {Boolean}, true if the file
        exists, false otherwise.
         */
        exists(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean}, true if the file exists, false otherwise.
         */
        existsSync(): boolean;
        /**
         * Get the SHA-1 digest of this file
         * @returns {string} Returns a promise that resolves to a {String}.
         */
        getDigest(): string;
        /**
         * Get the SHA-1 digest of this file
         * @returns {string} Returns a {String}.
         */
        getDigestSync(): string;
        /**
         * Sets the file's character set encoding name.
         * @param {string} The {String} encoding to use (default: 'utf8') 
         */
        setEncoding(encoding: string): void;
        /**
         * @returns {string} Returns the {String} encoding name for this file (default: 'utf8').
         */
        getEncoding(): string;
        /**
         * @returns {string} Returns the {String} path for the file.
         */
        getPath(): string;
        /**
         * @returns {string} Returns this file's completely resolved {String} path.
         */
        getRealPathSync(): string;
        /**
         * @returns {string} Returns a promise that resolves to the file's completely resolved {String} path.
         */
        getRealPath(): string;
        /**
         * Return the {String} filename without any directory information. 
         */
        getBaseName(): void;
        /**
         * Return the {Directory} that contains this file. 
         */
        getParent(): void;
        /**
         * Reads the contents of the file.
         * @param {boolean} A {Boolean} indicating whether to require a direct read or if a cached copy is acceptable.
         * @returns  Returns a promise that resovles to a String.
         */
        read(flushCache: boolean): any;
        /**
         * Overwrites the file with the given text.
         * @param {string} The {String} text to write to the underlying file.
         * @returns {Promise<any>} Returns a {Promise} that resolves when the file has been written.
         */
        write(text: string): Promise<any>;
        /**
         * Overwrites the file with the given text.
         * @param {string} The {String} text to write to the underlying file.
         * @returns  Returns .
         */
        writeSync(text: string): any;
    }

    /**
     * Represents the underlying git operations performed by Atom.
     * 
     * This class shouldn't be instantiated directly but instead by accessing the
     * `atom.project` global and calling `getRepo()`. Note that this will only be
     * available when the project is backed by a Git repository.
     * 
     * This class handles submodules automatically by taking a `path` argument to many
     * of the methods.  This `path` argument will determine which underlying
     * repository is used.
     * 
     * For a repository with submodules this would have the following outcome:
     * 
     * ```coffee
     * repo = atom.project.getRepo()
     * repo.getShortHead() # 'master'
     * repo.getShortHead('vendor/path/to/a/submodule') # 'dead1234'
     * ```
     *
     * file: src/git-repository.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/git-repository.coffee#L44
     */
    class GitRepository { 
        /**
         * Creates a new GitRepository instance.
         * @param {string} The {String} path to the Git repository to open.
         * @param {Object} An optional {Object} with the following keys:
         * @returns {GitRepository} Returns a {GitRepository} instance or `null` if the repository could not be opened.
         */
        static open(path: string, options: Object): GitRepository;

        /**
         * Destroy this {GitRepository} object.
         * 
         * This destroys any tasks and subscriptions and releases the underlying
         * libgit2 repository handle. This method is idempotent. 
         */
        destroy(): void;
        /**
         * Invoke the given callback when this GitRepository's destroy() method
         * is invoked. 
         */
        onDidDestroy(): void;
        /**
         * Invoke the given callback when a specific file's status has
         * changed. When a file is updated, reloaded, etc, and the status changes, this
         * will be fired.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeStatus(callback: Function): Disposable;
        /**
         * Invoke the given callback when a multiple files' statuses have
         * changed. For example, on window focus, the status of all the paths in the
         * repo is checked. If any of them have changed, this will be fired. Call
         * {::getPathStatus(path)} to get the status for your path of choice.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeStatuses(callback: Function): Disposable;
        /**
         * A {String} indicating the type of version control system used by
         * this repository.
         * @returns  Returns `"git"`.
         */
        getType(): any;
        /**
         * @returns {string} Returns the {String} path of the repository.
         */
        getPath(): string;
        /**
         * @returns {string} Returns the {String} working directory path of the repository.
         */
        getWorkingDirectory(): string;
        /**
         * @returns  Returns true if at the root, false if in a subfolder of the
        repository.
         */
        isProjectAtRoot(): any;
        /**
         * Makes a path relative to the repository's working directory. 
         */
        relativize(): void;
        /**
         * @returns  Returns true if the given branch exists.
         */
        hasBranch(): any;
        /**
         * Retrieves a shortened version of the HEAD reference value.
         * 
         * This removes the leading segments of `refs/heads`, `refs/tags`, or
         * `refs/remotes`.  It also shortens the SHA-1 of a detached `HEAD` to 7
         * characters.
         * @param {string} An optional {String} path in the repository to get this information for, only needed if the repository contains submodules.
         * @returns {string} Returns a {String}.
         */
        getShortHead(path: string): string;
        /**
         * Is the given path a submodule in the repository?
         * @param {string} The {String} path to check.
         * @returns {boolean} Returns a {Boolean}.
         */
        isSubmodule(path: string): boolean;
        /**
         * @param {string} The {String} branch reference name.
         * @param {string} The {String} path in the repository to get this information for, only needed if the repository contains submodules. 
         * @returns  Returns the number of commits behind the current branch is from the
        its upstream remote branch.
         */
        getAheadBehindCount(reference: string, path: string): any;
        /**
         * Get the cached ahead/behind commit counts for the current branch's
         * upstream branch.
         * @param {string} An optional {String} path in the repository to get this information for, only needed if the repository has submodules.
         * @returns {Object} Returns an {Object} with the following keys:
        
        * `ahead`  The {Number} of commits ahead.
        * `behind` The {Number} of commits behind.
         */
        getCachedUpstreamAheadBehindCount(path: string): Object;
        /**
         * @param {string} An optional {String} path in the repository to get this information for, only needed if the repository has submodules. 
         * @returns  Returns the git configuration value specified by the key.
         */
        getConfigValue(path: string): any;
        /**
         * @param {string} {String} path in the repository to get this information for, only needed if the repository has submodules. 
         * @returns  Returns the origin url of the repository.
         */
        getOriginURL(path?: string): any;
        /**
         * @param {string} An optional {String} path in the repo to get this information for, only needed if the repository contains submodules.
         * @returns  Returns the upstream branch for the current HEAD, or null if there
        is no upstream branch for the current HEAD.
         * @returns {string} Returns a {String} branch name such as `refs/remotes/origin/master`.
         */
        getUpstreamBranch(path: string): string;
        /**
         * Gets all the local and remote references.
         * @param {string} An optional {String} path in the repository to get this information for, only needed if the repository has submodules.
         * @returns {Object} Returns an {Object} with the following keys:
        
        * `heads`   An {Array} of head reference names.
        * `remotes` An {Array} of remote reference names.
        * `tags`    An {Array} of tag reference names.
         */
        getReferences(path: string): Object;
        /**
         * @param {string} The {String} reference to get the target of.
         * @param {string} An optional {String} path in the repo to get the reference target for. Only needed if the repository contains submodules. 
         * @returns {string} Returns the current {String} SHA for the given reference.
         */
        getReferenceTarget(reference: string, path: string): string;
        /**
         * @returns  Returns true if the given path is modified.
         */
        isPathModified(): any;
        /**
         * @returns  Returns true if the given path is new.
         */
        isPathNew(): any;
        /**
         * Is the given path ignored?
         * @returns {boolean} Returns a {Boolean}.
         */
        isPathIgnored(): boolean;
        /**
         * Get the status of a directory in the repository's working directory.
         * @param {string} The {String} path to check.
         * @returns {number} Returns a {Number} representing the status. This value can be passed to
        {::isStatusModified} or {::isStatusNew} to get more information.
         */
        getDirectoryStatus(path: string): number;
        /**
         * Get the status of a single path in the repository.
         * 
         * `path` A {String} repository-relative path.
         * @returns {number} Returns a {Number} representing the status. This value can be passed to
        {::isStatusModified} or {::isStatusNew} to get more information.
         */
        getPathStatus(): number;
        /**
         * Get the cached status for the given path.
         * @param {string} A {String} path in the repository, relative or absolute.
         * @returns {number} Returns a status {Number} or null if the path is not in the cache.
         */
        getCachedPathStatus(path: string): number;
        /**
         * @returns  Returns true if the given status indicates modification.
         */
        isStatusModified(): any;
        /**
         * @returns  Returns true if the given status indicates a new path.
         */
        isStatusNew(): any;
        /**
         * Retrieves the number of lines added and removed to a path.
         * 
         * This compares the working directory contents of the path to the `HEAD`
         * version.
         * @param {string} The {String} path to check.
         * @returns {Object} Returns an {Object} with the following keys:
        
        * `added` The {Number} of added lines.
        * `deleted` The {Number} of deleted lines.
         */
        getDiffStats(path: string): Object;
        /**
         * Retrieves the line diffs comparing the `HEAD` version of the given
         * path and the given text.
         * @param {string} The {String} path relative to the repository.
         * @param {string} The {String} to compare against the `HEAD` contents
         * @returns {any[]} Returns an {Array} of hunk {Object}s with the following keys:
        
        * `oldStart` The line {Number} of the old hunk.
        * `newStart` The line {Number} of the new hunk.
        * `oldLines` The {Number} of lines in the old hunk.
        * `newLines` The {Number} of lines in the new hunk
         */
        getLineDiffs(path: string, text: string): any[];
        /**
         * Restore the contents of a path in the working directory and index
         * to the version at `HEAD`.
         * 
         * This is essentially the same as running:
         * 
         * ```sh
         *   git reset HEAD -- <path>
         *   git checkout HEAD -- <path>
         * ```
         * @param {string} The {String} path to checkout.
         * @returns {boolean} Returns a {Boolean} that's true if the method was successful.
         */
        checkoutHead(path: string): boolean;
        /**
         * Checks out a branch in your repository.
         * @param {string} The {String} reference to checkout.
         * @param {boolean} A {Boolean} value which, if true creates the new reference if it doesn't exist.
         * @returns  Returns a Boolean that's true if the method was successful.
         */
        checkoutReference(reference: string, create: boolean): any;
    }

    /**
     * Grammar that tokenizes lines of text.
     * 
     * This class should not be instantiated directly but instead obtained from
     * a {GrammarRegistry} by calling {GrammarRegistry::loadGrammar}. 
     *
     * file: src/grammar.coffee
     * srcUrl: https://github.com/atom/first-mate/blob/v3.1.0/src/grammar.coffee#L21
     */
    class Grammar { 
        /**
         * Invoke the given callback when this grammar is updated due to a
         * grammar it depends on being added or removed from the registry.
         * @param {Function} {Function} to call when this grammar is updated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidUpdate(callback: Function): Disposable;
        /**
         * Tokenize all lines in the given text.
         * @param {string} A {String} containing one or more lines.
         * @returns {any[]} Returns an {Array} of token arrays for each line tokenized.
         */
        tokenizeLines(text: string): any[];
        /**
         * Tokenize the line of text.
         * @param {string} A {String} of text to tokenize.
         * @param {any[]} An optional {Array} of rules previously returned from this method. This should be null when tokenizing the first line in the file.
         * @param {boolean} A optional {Boolean} denoting whether this is the first line in the file which defaults to `false`. This should be `true` when tokenizing the first line in the file.
         * @returns {Object} Returns an {Object} containing the following properties:
        
        * `token` An {Array} of tokens covering the entire line of text.
        * `ruleStack` An {Array} of rules representing the tokenized state at the
          end of the line. These should be passed back into this method when
          tokenizing the next line in the file.
         */
        tokenizeLine(line: string, ruleStack: any[], firstLine: boolean): Object;
    }

    /**
     * Registry containing one or more grammars. 
     *
     * file: src/grammar-registry.coffee
     * srcUrl: https://github.com/atom/first-mate/blob/v3.1.0/src/grammar-registry.coffee#L11
     */
    class GrammarRegistry { 
        /**
         * Invoke the given callback when a grammar is added to the registry.
         * @param {Function} {Function} to call when a grammar is added.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddGrammar(callback: Function): Disposable;
        /**
         * Invoke the given callback when a grammar is updated due to a grammar
         * it depends on being added or removed from the registry.
         * @param {Function} {Function} to call when a grammar is updated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidUpdateGrammar(callback: Function): Disposable;
        /**
         * Get all the grammars in this registry.
         * @returns {any[]} Returns a non-empty {Array} of {Grammar} instances.
         */
        getGrammars(): any[];
        /**
         * Get a grammar with the given scope name.
         * @param {string} A {String} such as `"source.js"`.
         * @returns {Grammar} Returns a {Grammar} or .
         */
        grammarForScopeName(scopeName: string): Grammar;
        /**
         * Add a grammar to this registry.
         * 
         * A 'grammar-added' event is emitted after the grammar is added.
         * @param {Grammar} The {Grammar} to add. This should be a value previously returned from {::readGrammar} or {::readGrammarSync}.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        grammar.
         */
        addGrammar(grammar: Grammar): Disposable;
        /**
         * Remove the grammar with the given scope name.
         * @param {string} A {String} such as `"source.js"`.
         * @returns {Grammar} Returns the removed {Grammar} or .
         */
        removeGrammarForScopeName(scopeName: string): Grammar;
        /**
         * Read a grammar synchronously but don't add it to the registry.
         * @param {string} A {String} absolute file path to a grammar file.
         * @returns {Grammar} Returns a {Grammar}.
         */
        readGrammarSync(grammarPath: string): Grammar;
        /**
         * Read a grammar asynchronously but don't add it to the registry.
         * @param {string} A {String} absolute file path to a grammar file.
         * @param {Function} A {Function} to call when read with the following arguments:
         * @returns  Returns .
         */
        readGrammar(grammarPath: string, callback: Function): any;
        /**
         * Read a grammar synchronously and add it to this registry.
         * @param {string} A {String} absolute file path to a grammar file.
         * @returns {Grammar} Returns a {Grammar}.
         */
        loadGrammarSync(grammarPath: string): Grammar;
        /**
         * Read a grammar asynchronously and add it to the registry.
         * @param {string} A {String} absolute file path to a grammar file.
         * @param {Function} A {Function} to call when loaded with the following arguments:
         * @returns  Returns .
         */
        loadGrammar(grammarPath: string, callback: Function): any;
        /**
         * Get the grammar override for the given file path.
         * @param {string} A {String} file path.
         * @returns {Grammar} Returns a {Grammar} or .
         */
        grammarOverrideForPath(filePath: string): Grammar;
        /**
         * Set the grammar override for the given file path.
         * @param {string} A non-empty {String} file path.
         * @param {string} A {String} such as `"source.js"`.
         * @returns {Grammar} Returns a {Grammar} or .
         */
        setGrammarOverrideForPath(filePath: string, scopeName: string): Grammar;
        /**
         * Remove the grammar override for the given file path.
         * @param {string} A {String} file path.
         * @returns  Returns .
         */
        clearGrammarOverrideForPath(filePath: string): any;
        /**
         * Remove all grammar overrides.
         * @returns  Returns .
         */
        clearGrammarOverrides(): any;
        /**
         * Select a grammar for the given file path and file contents.
         * 
         * This picks the best match by checking the file path and contents against
         * each grammar.
         * @param {string} A {String} file path.
         * @param {string} A {String} of text for the file path.
         * @returns {Grammar} Returns a {Grammar}, never null.
         */
        selectGrammar(filePath: string, fileContents: string): Grammar;
    }

    /**
     * Allows commands to be associated with keystrokes in a
     * context-sensitive way. In Atom, you can access a global instance of this
     * object via `atom.keymap`.
     * 
     * Key bindings are plain JavaScript objects containing **CSS selectors** as
     * their top level keys, then **keystroke patterns** mapped to commands.
     * 
     * ```cson
     * '.workspace':
     *   'ctrl-l': 'package:do-something'
     *   'ctrl-z': 'package:do-something-else'
     * '.mini.editor':
     *   'enter': 'core:confirm'
     * ```
     * 
     * When a keystroke sequence matches a binding in a given context, a custom DOM
     * event with a type based on the command is dispatched on the target of the
     * keyboard event.
     * 
     * To match a keystroke sequence, the keymap starts at the target element for the
     * keyboard event. It looks for key bindings associated with selectors that match
     * the target element. If multiple match, the most specific is selected. If there
     * is a tie in specificity, the most recently added binding wins. If no bindings
     * are found for the events target, the search is repeated again for the target's
     * parent node and so on recursively until a binding is found or we traverse off
     * the top of the document.
     * 
     * When a binding is found, its command event is always dispatched on the
     * original target of the keyboard event, even if the matching element is higher
     * up in the DOM. In addition, `.preventDefault()` is called on the keyboard
     * event to prevent the browser from taking action. `.preventDefault` is only
     * called if a matching binding is found.
     * 
     * Command event objects have a non-standard method called `.abortKeyBinding()`.
     * If your command handler is invoked but you programmatically determine that no
     * action can be taken and you want to allow other bindings to be matched, call
     * `.abortKeyBinding()` on the event object. An example of where this is useful
     * is binding snippet expansion to `tab`. If `snippets:expand` is invoked when
     * the cursor does not follow a valid snippet prefix, we abort the binding and
     * allow `tab` to be handled by the default handler, which inserts whitespace.
     * 
     * Multi-keystroke bindings are possible. If a sequence of one or more keystrokes
     * *partially* matches a multi-keystroke binding, the keymap enters a pending
     * state. The pending state is terminated on the next keystroke, or after
     * {::partialMatchTimeout} milliseconds has elapsed. When the pending state is
     * terminated via a timeout or a keystroke that leads to no matches, the longest
     * ambiguous bindings that caused the pending state are temporarily disabled and
     * the previous keystrokes are replayed. If there is ambiguity again during the
     * replay, the next longest bindings are disabled and the keystrokes are replayed
     * again. 
     *
     * file: src/keymap-manager.coffee
     * srcUrl: https://github.com/atom/atom-keymap/blob/v5.1.2/src/keymap-manager.coffee#L67
     */
    class KeymapManager { 
        /**
         * Create a keydown DOM event for testing purposes.
         * @param  The key or keyIdentifier of the event. For example, `'a'`, `'1'`, `'escape'`, `'backspace'`, etc.
         * @param {Object} An {Object} containing any of the following:
         */
        static buildKeydownEvent(key: any, options?: Object): void;

        /**
         * Create a new KeymapManager.
         * @param {Object} An {Object} containing properties to assign to the keymap.  You can pass custom properties to be used by extension methods. The following properties are also supported:
         */
        constructor(options: Object);
        /**
         * Unwatch all watched paths. 
         */
        destroy(): void;
        /**
         * Invoke the given callback when one or more keystrokes completely
         * match a key binding.
         * @param {Function} {Function} to be called when keystrokes match a binding.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidMatchBinding(callback: Function): Disposable;
        /**
         * Invoke the given callback when one or more keystrokes partially
         * match a binding.
         * @param {Function} {Function} to be called when keystrokes partially match a binding.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidPartiallyMatchBindings(callback: Function): Disposable;
        /**
         * Invoke the given callback when one or more keystrokes fail to match
         * any bindings.
         * @param {Function} {Function} to be called when keystrokes fail to match any bindings.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidFailToMatchBinding(callback: Function): Disposable;
        /**
         * Invoke the given callback when a keymap file not able to be loaded.
         * @param {Function} {Function} to be called when a keymap file is unloaded.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidFailToReadFile(callback: Function): Disposable;
        /**
         * Add sets of key bindings grouped by CSS selector.
         * @param {string} A {String} (usually a path) uniquely identifying the given bindings so they can be removed later.
         * @param {Object} An {Object} whose top-level keys point at sub-objects mapping keystroke patterns to commands. 
         */
        add(source: string, bindings: Object): void;
        /**
         * Get all current key bindings.
         * @returns {any[]} Returns an {Array} of {KeyBinding}s.
         */
        getKeyBindings(): any[];
        /**
         * Get the key bindings for a given command and optional target.
         * @param {Object} An {Object} whose keys constrain the binding search:
         * @returns {any[]} Returns an {Array} of key bindings.
         */
        findKeyBindings(params: Object): any[];
        /**
         * Load the key bindings from the given path.
         * @param {string} A {String} containing a path to a file or a directory. If the path is a directory, all files inside it will be loaded.
         * @param {Object} An {Object} containing the following optional keys:
         */
        loadKeymap(path: string, options: Object): void;
        /**
         * Cause the keymap to reload the key bindings file at the given path
         * whenever it changes.
         * 
         * This method doesn't perform the initial load of the key bindings file. If
         * that's what you're looking for, call {::loadKeymap} with `watch: true`.
         * @param {string} A {String} containing a path to a file or a directory. If the path is a directory, all files inside it will be loaded. 
         */
        watchKeymap(path: string): void;
        /**
         * Dispatch a custom event associated with the matching key binding for
         * the given `KeyboardEvent` if one can be found.
         * 
         * If a matching binding is found on the event's target or one of its
         * ancestors, `.preventDefault()` is called on the keyboard event and the
         * binding's command is emitted as a custom event on the matching element.
         * 
         * If the matching binding's command is 'native!', the method will terminate
         * without calling `.preventDefault()` on the keyboard event, allowing the
         * browser to handle it as normal.
         * 
         * If the matching binding's command is 'unset!', the search will continue from
         * the current element's parent.
         * 
         * If the matching binding's command is 'abort!', the search will terminate
         * without dispatching a command event.
         * 
         * If the event's target is `document.body`, it will be treated as if its
         * target is `.defaultTarget` if that property is assigned on the keymap.
         * @param  A `KeyboardEvent` of type 'keydown' 
         */
        handleKeyboardEvent(event: any): void;
        /**
         * Translate a keydown event to a keystroke string.
         * @param  A `KeyboardEvent` of type 'keydown'
         * @returns {string} Returns a {String} describing the keystroke.
         */
        keystrokeForKeyboardEvent(event: any): string;
        /**
         * Get the number of milliseconds allowed before pending states caused
         * by partial matches of multi-keystroke bindings are terminated.
         * @returns {number} Returns a {Number}
         */
        getPartialMatchTimeout(): number;
    }

    /**
     * Represents a buffer annotation that remains logically stationary
     * even as the buffer changes. This is used to represent cursors, folds, snippet
     * targets, misspelled words, and anything else that needs to track a logical
     * location in the buffer over time.
     * 
     * ### Marker Creation
     * 
     * Use {TextEditor::markBufferRange} rather than creating Markers directly.
     * 
     * ### Head and Tail
     * 
     * Markers always have a *head* and sometimes have a *tail*. If you think of a
     * marker as an editor selection, the tail is the part that's stationary and the
     * head is the part that moves when the mouse is moved. A marker without a tail
     * always reports an empty range at the head position. A marker with a head position
     * greater than the tail is in a "normal" orientation. If the head precedes the
     * tail the marker is in a "reversed" orientation.
     * 
     * ### Validity
     * 
     * Markers are considered *valid* when they are first created. Depending on the
     * invalidation strategy you choose, certain changes to the buffer can cause a
     * marker to become invalid, for example if the text surrounding the marker is
     * deleted. The strategies, in order of descending fragility:
     * 
     * * __never__: The marker is never marked as invalid. This is a good choice for
     *   markers representing selections in an editor.
     * * __surround__: The marker is invalidated by changes that completely surround it.
     * * __overlap__: The marker is invalidated by changes that surround the
     *   start or end of the marker. This is the default.
     * * __inside__: The marker is invalidated by changes that extend into the
     *   inside of the marker. Changes that end at the marker's start or
     *   start at the marker's end do not invalidate the marker.
     * * __touch__: The marker is invalidated by a change that touches the marked
     *   region in any way, including changes that end at the marker's
     *   start or start at the marker's end. This is the most fragile strategy.
     * 
     * See {TextEditor::markBufferRange} for usage. 
     *
     * file: src/marker.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/marker.coffee#L44
     */
    class Marker { 
        /**
         * Destroys the marker, causing it to emit the 'destroyed' event. Once
         * destroyed, a marker cannot be restored by undo/redo operations. 
         */
        destroy(): void;
        /**
         * Creates and returns a new {Marker} with the same properties as this
         * marker.
         * @param {Object} {Object} 
         */
        copy(properties: Object): void;
        /**
         * Invoke the given callback when the state of the marker changes.
         * @param {Function} {Function} to be called when the marker changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChange(callback: Function): Disposable;
        /**
         * Invoke the given callback when the marker is destroyed.
         * @param {Function} {Function} to be called when the marker is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether the marker is valid. Markers can be
        invalidated when a region surrounding them in the buffer is changed.
         */
        isValid(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether the marker has been destroyed. A marker
        can be invalid without being destroyed, in which case undoing the invalidating
        operation would restore the marker. Once a marker is destroyed by calling
        {Marker::destroy}, no undo/redo operation can ever bring it back.
         */
        isDestroyed(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether the head precedes the tail.
         */
        isReversed(): boolean;
        /**
         * Get the invalidation strategy for this marker.
         * 
         * Valid values include: `never`, `surround`, `overlap`, `inside`, and `touch`.
         * @returns {string} Returns a {String}.
         */
        getInvalidationStrategy(): string;
        /**
         * @returns {Object} Returns an {Object} containing any custom properties associated with
        the marker.
         */
        getProperties(): Object;
        /**
         * Merges an {Object} containing new properties into the marker's
         * existing properties.
         * @param {Object} {Object} 
         */
        setProperties(properties: Object): void;
        /**
         * @param {Marker} {Marker} other marker 
         * @returns {boolean} Returns a {Boolean} indicating whether this marker is equivalent to
        another marker, meaning they have the same range and options.
         */
        isEqual(other: Marker): boolean;
        /**
         * Compares this marker to another based on their ranges.
         * @param {Marker} {Marker}
         * @returns {number} Returns a {Number}
         */
        compare(other: Marker): number;
        /**
         * Gets the buffer range of the display marker.
         * @returns {Range} Returns a {Range}.
         */
        getBufferRange(): Range;
        /**
         * Modifies the buffer range of the display marker.
         * @param {Range} The new {Range} to use
         * @param {Object} {Object} properties to associate with the marker.
         */
        setBufferRange(bufferRange: Range, properties?: Object): void;
        /**
         * Gets the screen range of the display marker.
         * @returns {Range} Returns a {Range}.
         */
        getScreenRange(): Range;
        /**
         * Modifies the screen range of the display marker.
         * @param {Range} The new {Range} to use
         * @param {Object} {Object} properties to associate with the marker.
         */
        setScreenRange(screenRange: Range, properties?: Object): void;
        /**
         * Retrieves the buffer position of the marker's start. This will always be
         * less than or equal to the result of {Marker::getEndBufferPosition}.
         * @returns {Point} Returns a {Point}.
         */
        getStartBufferPosition(): Point;
        /**
         * Retrieves the screen position of the marker's start. This will always be
         * less than or equal to the result of {Marker::getEndScreenPosition}.
         * @returns {Point} Returns a {Point}.
         */
        getStartScreenPosition(): Point;
        /**
         * Retrieves the buffer position of the marker's end. This will always be
         * greater than or equal to the result of {Marker::getStartBufferPosition}.
         * @returns {Point} Returns a {Point}.
         */
        getEndBufferPosition(): Point;
        /**
         * Retrieves the screen position of the marker's end. This will always be
         * greater than or equal to the result of {Marker::getStartScreenPosition}.
         * @returns {Point} Returns a {Point}.
         */
        getEndScreenPosition(): Point;
        /**
         * Retrieves the buffer position of the marker's head.
         * @returns {Point} Returns a {Point}.
         */
        getHeadBufferPosition(): Point;
        /**
         * Sets the buffer position of the marker's head.
         * @param {Point} The new {Point} to use
         * @param {Object} {Object} properties to associate with the marker. 
         */
        setHeadBufferPosition(bufferPosition: Point, properties?: Object): void;
        /**
         * Retrieves the screen position of the marker's head.
         * @returns {Point} Returns a {Point}.
         */
        getHeadScreenPosition(): Point;
        /**
         * Sets the screen position of the marker's head.
         * @param {Point} The new {Point} to use
         * @param {Object} {Object} properties to associate with the marker. 
         */
        setHeadScreenPosition(screenPosition: Point, properties?: Object): void;
        /**
         * Retrieves the buffer position of the marker's tail.
         * @returns {Point} Returns a {Point}.
         */
        getTailBufferPosition(): Point;
        /**
         * Sets the buffer position of the marker's tail.
         * @param {Point} The new {Point} to use
         * @param {Object} {Object} properties to associate with the marker. 
         */
        setTailBufferPosition(bufferPosition: Point, properties?: Object): void;
        /**
         * Retrieves the screen position of the marker's tail.
         * @returns {Point} Returns a {Point}.
         */
        getTailScreenPosition(): Point;
        /**
         * Sets the screen position of the marker's tail.
         * @param {Point} The new {Point} to use
         * @param {Object} {Object} properties to associate with the marker. 
         */
        setTailScreenPosition(screenPosition: Point, properties?: Object): void;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether the marker has a tail.
         */
        hasTail(): boolean;
        /**
         * Plants the marker's tail at the current head position. After calling
         * the marker's tail position will be its head position at the time of the
         * call, regardless of where the marker's head is moved.
         * @param {Object} {Object} properties to associate with the marker. 
         */
        plantTail(properties?: Object): void;
        /**
         * Removes the marker's tail. After calling the marker's head position
         * will be reported as its current tail position until the tail is planted
         * again.
         * @param {Object} {Object} properties to associate with the marker. 
         */
        clearTail(properties?: Object): void;
    }

    /**
     * Provides a registry for menu items that you'd like to appear in the
     * application menu.
     * 
     * An instance of this class is always available as the `atom.menu` global.
     * 
     * ## Menu CSON Format
     * 
     * Here is an example from the [tree-view](https://github.com/atom/tree-view/blob/master/menus/tree-view.cson):
     * 
     * ```coffee
     * [
     *   {
     *     'label': 'View'
     *     'submenu': [
     *       { 'label': 'Toggle Tree View', 'command': 'tree-view:toggle' }
     *     ]
     *   }
     *   {
     *     'label': 'Packages'
     *     'submenu': [
     *       'label': 'Tree View'
     *       'submenu': [
     *         { 'label': 'Focus', 'command': 'tree-view:toggle-focus' }
     *         { 'label': 'Toggle', 'command': 'tree-view:toggle' }
     *         { 'label': 'Reveal Active File', 'command': 'tree-view:reveal-active-file' }
     *         { 'label': 'Toggle Tree Side', 'command': 'tree-view:toggle-side' }
     *       ]
     *     ]
     *   }
     * ]
     * ```
     * 
     * Use in your package's menu `.cson` file requires that you place your menu
     * structure under a `menu` key.
     * 
     * ```coffee
     * 'menu': [
     *   {
     *     'label': 'View'
     *     'submenu': [
     *       { 'label': 'Toggle Tree View', 'command': 'tree-view:toggle' }
     *     ]
     *   }
     * ]
     * ```
     * 
     * See {::add} for more info about adding menu's directly. 
     *
     * file: src/menu-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/menu-manager.coffee#L61
     */
    class MenuManager { 
        /**
         * Adds the given items to the application menu.
         * @param {any[]} An {Array} of menu item {Object}s containing the keys:
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        added menu items.
         */
        add(items: any[]): Disposable;
        /**
         * Refreshes the currently visible menu. 
         */
        update(): void;
    }

    /**
     * Package manager for coordinating the lifecycle of Atom packages.
     * 
     * An instance of this class is always available as the `atom.packages` global.
     * 
     * Packages can be loaded, activated, and deactivated, and unloaded:
     * 
     * * Loading a package reads and parses the package's metadata and resources
     *   such as keymaps, menus, stylesheets, etc.
     * * Activating a package registers the loaded resources and calls `activate()`
     *   on the package's main module.
     * * Deactivating a package unregisters the package's resources  and calls
     *   `deactivate()` on the package's main module.
     * * Unloading a package removes it completely from the package manager.
     * 
     * Packages can be enabled/disabled via the `core.disabledPackages` config
     * settings and also by calling `enablePackage()/disablePackage()`. 
     *
     * file: src/package-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/package-manager.coffee#L29
     */
    class PackageManager { 
        /**
         * Invoke the given callback when all packages have been loaded.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidLoadInitialPackages(callback: Function): Disposable;
        /**
         * Invoke the given callback when all packages have been activated.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidActivateInitialPackages(callback: Function): Disposable;
        /**
         * Invoke the given callback when a package is activated.
         * @param {Function} A {Function} to be invoked when a package is activated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidActivatePackage(callback: Function): Disposable;
        /**
         * Invoke the given callback when a package is deactivated.
         * @param {Function} A {Function} to be invoked when a package is deactivated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDeactivatePackage(callback: Function): Disposable;
        /**
         * Invoke the given callback when a package is loaded.
         * @param {Function} A {Function} to be invoked when a package is loaded.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidLoadPackage(callback: Function): Disposable;
        /**
         * Invoke the given callback when a package is unloaded.
         * @param {Function} A {Function} to be invoked when a package is unloaded.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidUnloadPackage(callback: Function): Disposable;
        /**
         * Get the path to the apm command.
         * 
         * Return a {String} file path to apm. 
         */
        getApmPath(): void;
        /**
         * Get the paths being used to look for packages.
         * @returns {any[]} Returns an {Array} of {String} directory paths.
         */
        getPackageDirPaths(): any[];
        /**
         * Resolve the given package name to a path on disk.
         * 
         * Return a {String} folder path or undefined if it could not be resolved. 
         * @param {string} The {String} package name.
         */
        resolvePackagePath(name: string): void;
        /**
         * Is the package with the given name bundled with Atom?
         * @param {string} The {String} package name.
         * @returns {boolean} Returns a {Boolean}.
         */
        isBundledPackage(name: string): boolean;
        /**
         * Enable the package with the given name.
         * @returns {Package} Returns the {Package} that was enabled or null if it isn't loaded.
         */
        enablePackage(): Package;
        /**
         * Disable the package with the given name.
         * @returns {Package} Returns the {Package} that was disabled or null if it isn't loaded.
         */
        disablePackage(): Package;
        /**
         * Is the package with the given name disabled?
         * @param {string} The {String} package name.
         * @returns {boolean} Returns a {Boolean}.
         */
        isPackageDisabled(name: string): boolean;
        /**
         * Get an {Array} of all the active {Package}s. 
         */
        getActivePackages(): void;
        /**
         * Get the active {Package} with the given name.
         * @param {string} The {String} package name.
         * @returns {Package} Returns a {Package} or .
         */
        getActivePackage(name: string): Package;
        /**
         * Is the {Package} with the given name active?
         * @param {string} The {String} package name.
         * @returns {boolean} Returns a {Boolean}.
         */
        isPackageActive(name: string): boolean;
        /**
         * Get an {Array} of all the loaded {Package}s 
         */
        getLoadedPackages(): void;
        /**
         * Get the loaded {Package} with the given name.
         * @param {string} The {String} package name.
         * @returns {Package} Returns a {Package} or .
         */
        getLoadedPackage(name: string): Package;
        /**
         * Is the package with the given name loaded?
         * @param {string} The {String} package name.
         * @returns {boolean} Returns a {Boolean}.
         */
        isPackageLoaded(name: string): boolean;
        /**
         * Get an {Array} of {String}s of all the available package paths. 
         */
        getAvailablePackagePaths(): void;
        /**
         * Get an {Array} of {String}s of all the available package names. 
         */
        getAvailablePackageNames(): void;
        /**
         * Get an {Array} of {String}s of all the available package metadata. 
         */
        getAvailablePackageMetadata(): void;
    }

    /**
     * A container for presenting content in the center of the workspace.
     * Panes can contain multiple items, one of which is *active* at a given time.
     * The view corresponding to the active item is displayed in the interface. In
     * the default configuration, tabs are also displayed for each item. 
     *
     * file: src/pane.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/pane.coffee#L14
     */
    class Pane { 
        /**
         * Invoke the given callback when the pane resize
         * 
         * the callback will be invoked when pane's flexScale property changes
         * @param {Function} {Function} to be called when the pane is resized
         * @returns {Disposable} Returns a {Disposable} on which '.dispose()' can be called to unsubscribe.
         */
        onDidChangeFlexScale(callback: Function): Disposable;
        /**
         * Invoke the given callback with all current and future items.
         * @param {Function} {Function} to be called with current and future items.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeFlexScale(callback: Function): Disposable;
        /**
         * Invoke the given callback when the pane is activated.
         * 
         * The given callback will be invoked whenever {::activate} is called on the
         * pane, even if it is already active at the time.
         * @param {Function} {Function} to be called when the pane is activated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidActivate(callback: Function): Disposable;
        /**
         * Invoke the given callback when the pane is destroyed.
         * @param {Function} {Function} to be called when the pane is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * Invoke the given callback when the value of the {::isActive}
         * property changes.
         * @param {Function} {Function} to be called when the value of the {::isActive} property changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeActive(callback: Function): Disposable;
        /**
         * Invoke the given callback with the current and future values of the
         * {::isActive} property.
         * @param {Function} {Function} to be called with the current and future values of the {::isActive} property.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeActive(callback: Function): Disposable;
        /**
         * Invoke the given callback when an item is added to the pane.
         * @param {Function} {Function} to be called with when items are added.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddItem(callback: Function): Disposable;
        /**
         * Invoke the given callback when an item is removed from the pane.
         * @param {Function} {Function} to be called with when items are removed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidRemoveItem(callback: Function): Disposable;
        /**
         * Invoke the given callback when an item is moved within the pane.
         * @param {Function} {Function} to be called with when items are moved.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidMoveItem(callback: Function): Disposable;
        /**
         * Invoke the given callback with all current and future items.
         * @param {Function} {Function} to be called with current and future items.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeItems(callback: Function): Disposable;
        /**
         * Invoke the given callback when the value of {::getActiveItem}
         * changes.
         * @param {Function} {Function} to be called with when the active item changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeActiveItem(callback: Function): Disposable;
        /**
         * Invoke the given callback with the current and future values of
         * {::getActiveItem}.
         * @param {Function} {Function} to be called with the current and future active items.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeActiveItem(callback: Function): Disposable;
        /**
         * Invoke the given callback before items are destroyed.
         * @param {Function} {Function} to be called before items are destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to
        unsubscribe.
         */
        onWillDestroyItem(callback: Function): Disposable;
        /**
         * Get the items in this pane.
         * @returns {any[]} Returns an {Array} of items.
         */
        getItems(): any[];
        /**
         * Get the active pane item in this pane.
         * @returns  Returns a pane item.
         */
        getActiveItem(): any;
        /**
         * Return the item at the given index.
         * @param {number} {Number}
         * @returns  Returns an item or `null` if no item exists at the given index.
         */
        itemAtIndex(index: number): any;
        /**
         * Makes the next item active. 
         */
        activateNextItem(): void;
        /**
         * Makes the previous item active. 
         */
        activatePreviousItem(): void;
        /**
         * Move the active tab to the right. 
         */
        moveItemRight(): void;
        /**
         * Move the active tab to the left 
         */
        moveItemLeft(): void;
        /**
         * Get the index of the active item.
         * @returns {number} Returns a {Number}.
         */
        getActiveItemIndex(): number;
        /**
         * Activate the item at the given index.
         * @param {number} {Number} 
         */
        activateItemAtIndex(index: number): void;
        /**
         * Make the given item *active*, causing it to be displayed by
         * the pane's view. 
         */
        activateItem(): void;
        /**
         * Add the given item to the pane.
         * @param  The item to add. It can be a model with an associated view or a view.
         * @param {number} {Number} indicating the index at which to add the item. If omitted, the item is added after the current active item.
         * @returns  Returns the added item.
         */
        addItem(item: any, index?: number): any;
        /**
         * Add the given items to the pane.
         * @param {any[]} An {Array} of items to add. Items can be views or models with associated views. Any objects that are already present in the pane's current items will not be added again.
         * @param {number} {Number} index at which to add the items. If omitted, the item is #   added after the current active item.
         * @returns {any[]} Returns an {Array} of added items.
         */
        addItems(items: any[], index?: number): any[];
        /**
         * Move the given item to the given index.
         * @param  The item to move.
         * @param {number} {Number} indicating the index to which to move the item. 
         */
        moveItem(item: any, index: number): void;
        /**
         * Move the given item to the given index on another pane.
         * @param  The item to move.
         * @param {Pane} {Pane} to which to move the item.
         * @param {number} {Number} indicating the index to which to move the item in the given pane. 
         */
        moveItemToPane(item: any, pane: Pane, index: number): void;
        /**
         * Destroy the active item and activate the next item. 
         */
        destroyActiveItem(): void;
        /**
         * Destroy the given item.
         * 
         * If the item is active, the next item will be activated. If the item is the
         * last item, the pane will be destroyed if the `core.destroyEmptyPanes` config
         * setting is `true`.
         * @param  Item to destroy 
         */
        destroyItem(item: any): void;
        /**
         * Destroy all items. 
         */
        destroyItems(): void;
        /**
         * Destroy all items except for the active item. 
         */
        destroyInactiveItems(): void;
        /**
         * Save the active item. 
         */
        saveActiveItem(): void;
        /**
         * Prompt the user for a location and save the active item with the
         * path they select.
         * @param {Function} {Function} which will be called after the item is successfully saved. 
         */
        saveActiveItemAs(nextAction?: Function): void;
        /**
         * Save the given item.
         * @param  The item to save.
         * @param {Function} {Function} which will be called after the item is successfully saved. 
         */
        saveItem(item: any, nextAction?: Function): void;
        /**
         * Prompt the user for a location and save the active item with the
         * path they select.
         * @param  The item to save.
         * @param {Function} {Function} which will be called after the item is successfully saved. 
         */
        saveItemAs(item: any, nextAction?: Function): void;
        /**
         * Save all items. 
         */
        saveItems(): void;
        /**
         * Return the first item that matches the given URI or undefined if
         * none exists.
         * @param {string} {String} containing a URI. 
         */
        itemForURI(uri: string): void;
        /**
         * Activate the first item that matches the given URI.
         * @returns {boolean} Returns a {Boolean} indicating whether an item matching the URI was found.
         */
        activateItemForURI(): boolean;
        /**
         * Determine whether the pane is active.
         * @returns {boolean} Returns a {Boolean}.
         */
        isActive(): boolean;
        /**
         * Makes this pane the *active* pane, causing it to gain focus. 
         */
        activate(): void;
        /**
         * Close the pane and destroy all its items.
         * 
         * If this is the last pane, all the items will be destroyed but the pane
         * itself will not be destroyed. 
         */
        destroy(): void;
        /**
         * Create a new pane to the left of this pane.
         * @param {Object} {Object} with the following keys:
         * @returns {Pane} Returns the new {Pane}.
         */
        splitLeft(params?: Object): Pane;
        /**
         * Create a new pane to the right of this pane.
         * @param {Object} {Object} with the following keys:
         * @returns {Pane} Returns the new {Pane}.
         */
        splitRight(params?: Object): Pane;
        /**
         * Creates a new pane above the receiver.
         * @param {Object} {Object} with the following keys:
         * @returns {Pane} Returns the new {Pane}.
         */
        splitUp(params?: Object): Pane;
        /**
         * Creates a new pane below the receiver.
         * @param {Object} {Object} with the following keys:
         * @returns {Pane} Returns the new {Pane}.
         */
        splitDown(params?: Object): Pane;
    }

    /**
     * A container representing a panel on the edges of the editor window.
     * You should not create a `Panel` directly, instead use {Workspace::addTopPanel}
     * and friends to add panels.
     * 
     * Examples: [tree-view](https://github.com/atom/tree-view),
     * [status-bar](https://github.com/atom/status-bar),
     * and [find-and-replace](https://github.com/atom/find-and-replace) all use
     * panels. 
     *
     * file: src/panel.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/panel.coffee#L12
     */
    class Panel { 
        /**
         * Destroy and remove this panel from the UI. 
         */
        destroy(): void;
        /**
         * Invoke the given callback when the pane hidden or shown.
         * @param {Function} {Function} to be called when the pane is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeVisible(callback: Function): Disposable;
        /**
         * Invoke the given callback when the pane is destroyed.
         * @param {Function} {Function} to be called when the pane is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * @returns  Returns the panel's item.
         */
        getItem(): any;
        /**
         * @returns {number} Returns a {Number} indicating this panel's priority.
         */
        getPriority(): number;
        /**
         * @returns {boolean} Returns a {Boolean} true when the panel is visible.
         */
        isVisible(): boolean;
        /**
         * Hide this panel 
         */
        hide(): void;
        /**
         * Show this panel 
         */
        show(): void;
    }

    /**
     * Represents a point in a buffer in row/column coordinates.
     * 
     * Every public method that takes a point also accepts a *point-compatible*
     * {Array}. This means a 2-element array containing {Number}s representing the
     * row and column. So the following are equivalent:
     * 
     * ```coffee
     * new Point(1, 2)
     * [1, 2] # Point compatible Array
     * ```
     *
     * file: src/point.coffee
     * srcUrl: https://github.com/atom/text-buffer/blob/v5.2.0/src/point.coffee#L14
     */
    class Point { 
        /**
         * Convert any point-compatible object to a {Point}.
         * @param {Point} This can be an object that's already a {Point}, in which case it's simply returned, or an array containing two {Number}s representing the row and column.
         * @param  An optional boolean indicating whether to force the copying of objects that are already points.
         * @returns {Point} Returns: A {Point} based on the given object.
         */
        static fromObject(object: Point, copy: any): Point;
        /**
         * @param {Point} {Point}
         * @param {Point} {Point} 
         * @returns {Point} Returns the given {Point} that is earlier in the buffer.
         */
        static min(point1: Point, point2: Point): Point;

        /**
         * Construct a {Point} object
         * @param {number} {Number} row
         * @param {number} {Number} column 
         */
        constructor(row: number, column: number);
        /**
         * @returns {Point} Returns a new {Point} with the same row and column.
         */
        copy(): Point;
        /**
         * @returns {Point} Returns a new {Point} with the row and column negated.
         */
        negate(): Point;
        /**
         * Makes this point immutable and returns itself.
         * @returns {Point} Returns an immutable version of this {Point}
         */
        freeze(): Point;
        /**
         * Build and return a new point by adding the rows and columns of
         * the given point.
         * @param {Point} A {Point} whose row and column will be added to this point's row and column to build the returned point.
         * @returns {Point} Returns a {Point}.
         */
        translate(other: Point): Point;
        /**
         * Build and return a new {Point} by traversing the rows and columns
         * specified by the given point.
         * 
         * This method differs from the direct, vector-style addition offered by
         * {::translate}. Rather than adding the rows and columns directly, it derives
         * the new point from traversing in "typewriter space". At the end of every row
         * traversed, a carriage return occurs that returns the columns to 0 before
         * continuing the traversal.
         * @param {Point} A {Point} providing the rows and columns to traverse by.
         * @returns {Point} Returns a {Point}.
         */
        traverse(other: Point): Point;
        /**
         * @param {Point} A {Point} or point-compatible {Array}.
         * @returns  Returns `-1` if this point precedes the argument.
         * @returns  Returns `0` if this point is equivalent to the argument.
         * @returns  Returns `1` if this point follows the argument.
         */
        compare(other: Point): any;
        /**
         * @param {Point} A {Point} or point-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this point has the same row
        and column as the given {Point} or point-compatible {Array}.
         */
        isEqual(other: Point): boolean;
        /**
         * @param {Point} A {Point} or point-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this point precedes the given
        {Point} or point-compatible {Array}.
         */
        isLessThan(other: Point): boolean;
        /**
         * @param {Point} A {Point} or point-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this point precedes or is
        equal to the given {Point} or point-compatible {Array}.
         */
        isLessThanOrEqual(other: Point): boolean;
        /**
         * @param {Point} A {Point} or point-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this point follows the given
        {Point} or point-compatible {Array}.
         */
        isGreaterThan(other: Point): boolean;
        /**
         * @param {Point} A {Point} or point-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this point follows or is
        equal to the given {Point} or point-compatible {Array}.
         */
        isGreaterThanOrEqual(other: Point): boolean;
        /**
         * @returns  Returns an array of this point's row and column.
         */
        toArray(): any;
        /**
         * @returns  Returns an array of this point's row and column.
         */
        serialize(): any;
        /**
         * @returns  Returns a string representation of the point.
         */
        toString(): any;
    }

    /**
     * Represents a project that's opened in Atom.
     * 
     * An instance of this class is always available as the `atom.project` global. 
     *
     * file: src/project.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/project.coffee#L23
     */
    class Project { 
        /**
         * Invoke the given callback when the project paths change.
         * @param {Function} {Function} to be called after the project paths change.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangePaths(callback: Function): Disposable;
        /**
         * Get an {Array} of {GitRepository}s associated with the project's
         * directories.
         * 
         * This method will be removed in 2.0 because it does synchronous I/O.
         * Prefer the following, which evaluates to a {Promise} that resolves to an
         * {Array} of {Repository} objects:
         * 
         * ```
         * Promise.all(project.getDirectories().map(
         *     project.repositoryForDirectory.bind(project)))
         * ```
         */
        getRepositories(): void;
        /**
         * Get the repository for a given directory asynchronously.
         * @param {Directory} {Directory} for which to get a {Repository}.
         * @returns {Promise<any>} Returns a {Promise} that resolves with either:
        
        * {Repository} if a repository can be created for the given directory
        * `null` if no repository can be created for the given directory.
         */
        repositoryForDirectory(directory: Directory): Promise<any>;
        /**
         * Get an {Array} of {String}s containing the paths of the project's
         * directories. 
         */
        getPaths(): void;
        /**
         * Set the paths of the project's directories.
         * @param {any[]} {Array} of {String} paths. 
         */
        setPaths(projectPaths: any[]): void;
        /**
         * Add a path to the project's list of root paths
         * @param {string} {String} The path to the directory to add. 
         */
        addPath(projectPath: string): void;
        /**
         * remove a path from the project's list of root paths.
         * @param {string} {String} The path to remove. 
         */
        removePath(projectPath: string): void;
        /**
         * Get an {Array} of {Directory}s associated with this project. 
         */
        getDirectories(): void;
        /**
         * Get the path to the project directory that contains the given path,
         * and the relative path from that project directory to the given path.
         * @param {string} {String} An absolute path.
         * @returns {any[]} Returns an {Array} with two elements:
        
        * `projectPath` The {String} path to the project directory that contains the
          given path, or `null` if none is found.
        * `relativePath` {String} The relative path from the project directory to
          the given path.
         */
        relativizePath(fullPath: string): any[];
        /**
         * Determines whether the given path (real or symbolic) is inside the
         * project's directory.
         * 
         * This method does not actually check if the path exists, it just checks their
         * locations relative to each other.
         * @param {string} {String} path
         * @returns  Returns whether the path is inside the project's root directory.
         */
        contains(pathToCheck: string): any;
    }

    /**
     * Represents a region in a buffer in row/column coordinates.
     * 
     * Every public method that takes a range also accepts a *range-compatible*
     * {Array}. This means a 2-element array containing {Point}s or point-compatible
     * arrays. So the following are equivalent:
     *
     * file: src/range.coffee
     * srcUrl: https://github.com/atom/text-buffer/blob/v5.2.0/src/range.coffee#L19
     */
    class Range { 
        /**
         * Convert any range-compatible object to a {Range}.
         * @param {Range} This can be an object that's already a {Range}, in which case it's simply returned, or an array containing two {Point}s or point-compatible arrays.
         * @param  An optional boolean indicating whether to force the copying of objects that are already ranges.˚
         * @returns {Range} Returns: A {Range} based on the given object.
         */
        static fromObject(object: Range, copy: any): Range;
        /**
         * Call this with the result of {Range::serialize} to construct a new Range.
         * @param {any[]} {array} of params to pass to the {::constructor} 
         */
        static deserialize(array: any[]): void;

        /**
         * Construct a {Range} object
         * @param {Point} {Point} or Point compatible {Array} (default: [0,0])
         * @param {Point} {Point} or Point compatible {Array} (default: [0,0]) 
         */
        constructor(pointA: Point, pointB: Point);
        /**
         * @returns  Returns a new range with the same start and end positions.
         */
        copy(): any;
        /**
         * @returns  Returns a new range with the start and end positions negated.
         */
        negate(): any;
        /**
         * @returns  Returns a plain javascript object representation of the range.
         */
        serialize(): any;
        /**
         * Is the start position of this range equal to the end position?
         * @returns {boolean} Returns a {Boolean}.
         */
        isEmpty(): boolean;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether this range starts and ends on
        the same row.
         */
        isSingleLine(): boolean;
        /**
         * Get the number of rows in this range.
         * @returns {number} Returns a {Number}.
         */
        getRowCount(): number;
        /**
         * @returns  Returns an array of all rows in the range.
         */
        getRows(): any;
        /**
         * Freezes the range and its start and end point so it becomes
         * immutable and returns itself.
         * @returns {Range} Returns an immutable version of this {Range}
         */
        freeze(): Range;
        /**
         * @param {Range} A {Range} or range-compatible {Array} 
         * @returns  Returns a new range that contains this range and the given range.
         */
        union(otherRange: Range): any;
        /**
         * Build and return a new range by translating this range's start and
         * end points by the given delta(s).
         * @param {Point} A {Point} by which to translate the start of this range.
         * @param {Point} A {Point} to by which to translate the end of this range. If omitted, the `startDelta` will be used instead.
         * @returns {Range} Returns a {Range}.
         */
        translate(startDelta: Point, endDelta?: Point): Range;
        /**
         * Build and return a new range by traversing this range's start and
         * end points by the given delta.
         * 
         * See {Point::traverse} for details of how traversal differs from translation.
         * @param {Point} A {Point} containing the rows and columns to traverse to derive the new range.
         * @returns {Range} Returns a {Range}.
         */
        traverse(delta: Point): Range;
        /**
         * Compare two Ranges
         * @param {Range} A {Range} or range-compatible {Array}.
         * @returns  Returns `-1` if this range starts before the argument or contains it.
         * @returns  Returns `0` if this range is equivalent to the argument.
         * @returns  Returns `1` if this range starts after the argument or is contained by it.
         */
        compare(otherRange: Range): any;
        /**
         * @param {Range} A {Range} or range-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this range has the same start
        and end points as the given {Range} or range-compatible {Array}.
         */
        isEqual(otherRange: Range): boolean;
        /**
         * @param {Range} A {Range} or range-compatible {Array}. 
         * @returns {boolean} Returns a {Boolean} indicating whether this range starts and ends on
        the same row as the argument.
         */
        coversSameRows(otherRange: Range): boolean;
        /**
         * Determines whether this range intersects with the argument.
         * @param {Range} A {Range} or range-compatible {Array}
         * @param {boolean} {Boolean} indicating whether to exclude endpoints   when testing for intersection. Defaults to `false`.
         * @returns {boolean} Returns a {Boolean}.
         */
        intersectsWith(otherRange: Range, exclusive?: boolean): boolean;
        /**
         * @param {Range} A {Range} or range-compatible {Array}
         * @param  A boolean value including that the containment should be exclusive of endpoints. Defaults to false. 
         * @returns {boolean} Returns a {Boolean} indicating whether this range contains the given
        range.
         */
        containsRange(otherRange: Range, exclusive: any): boolean;
        /**
         * @param {Point} A {Point} or point-compatible {Array}
         * @param  A boolean value including that the containment should be exclusive of endpoints. Defaults to false. 
         * @returns {boolean} Returns a {Boolean} indicating whether this range contains the given
        point.
         */
        containsPoint(point: Point, exclusive: any): boolean;
        /**
         * @param {number} Row {Number} 
         * @returns {boolean} Returns a {Boolean} indicating whether this range intersects the
        given row {Number}.
         */
        intersectsRow(row: number): boolean;
        /**
         * @param {number} {Number} start row
         * @param {number} {Number} end row 
         * @returns {boolean} Returns a {Boolean} indicating whether this range intersects the
        row range indicated by the given startRow and endRow {Number}s.
         */
        intersectsRowRange(startRow: number, endRow: number): boolean;
        /**
         * @returns  Returns a string representation of the range.
         */
        toString(): any;
    }

    /**
     * Wraps an {Array} of `String`s. The Array describes a path from the
     * root of the syntax tree to a token including _all_ scope names for the entire
     * path.
     * 
     * Methods that take a `ScopeDescriptor` will also accept an {Array} of {Strings}
     * scope names e.g. `['.source.js']`.
     * 
     * You can use `ScopeDescriptor`s to get language-specific config settings via
     * {Config::get}.
     * 
     * You should not need to create a `ScopeDescriptor` directly.
     * 
     * * {Editor::getRootScopeDescriptor} to get the language's descriptor.
     * * {Editor::scopeDescriptorForBufferPosition} to get the descriptor at a
     *   specific position in the buffer.
     * * {Cursor::getScopeDescriptor} to get a cursor's descriptor based on position.
     * 
     * See the [scopes and scope descriptor guide](https://atom.io/docs/latest/behind-atom-scoped-settings-scopes-and-scope-descriptors)
     * for more information. 
     *
     * file: src/scope-descriptor.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/scope-descriptor.coffee#L21
     */
    class ScopeDescriptor { 
        /**
         * Create a {ScopeDescriptor} object.
         * @param {Object} {Object}
         */
        constructor(object: Object);
        /**
         * @returns {any[]} Returns an {Array} of {String}s
         */
        getScopesArray(): any[];
    }

    /**
     * Represents a selection in the {TextEditor}. 
     *
     * file: src/selection.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/selection.coffee#L11
     */
    class Selection { 
        /**
         * Calls your `callback` when the selection was moved.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeRange(callback: Function): Disposable;
        /**
         * Calls your `callback` when the selection was destroyed
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * @returns {Range} Returns the screen {Range} for the selection.
         */
        getScreenRange(): Range;
        /**
         * Modifies the screen range for the selection.
         * @param {Range} The new {Range} to use.
         * @param {Object} {Object} options matching those found in {::setBufferRange}. 
         */
        setScreenRange(screenRange: Range, options?: Object): void;
        /**
         * @returns {Range} Returns the buffer {Range} for the selection.
         */
        getBufferRange(): Range;
        /**
         * Modifies the buffer {Range} for the selection.
         * @param {Range} The new {Range} to select.
         * @param {Object} {Object} with the keys:
         */
        setBufferRange(screenRange: Range, options?: Object): void;
        /**
         * @returns  Returns the starting and ending buffer rows the selection is
        highlighting.
         * @returns {any[]} Returns an {Array} of two {Number}s: the starting row, and the ending row.
         */
        getBufferRowRange(): any[];
        /**
         * Determines if the selection contains anything. 
         */
        isEmpty(): void;
        /**
         * Determines if the ending position of a marker is greater than the
         * starting position.
         * 
         * This can happen when, for example, you highlight text "up" in a {TextBuffer}. 
         */
        isReversed(): void;
        /**
         * @returns  Returns whether the selection is a single line or not.
         */
        isSingleScreenLine(): any;
        /**
         * @returns  Returns the text in the selection.
         */
        getText(): any;
        /**
         * Identifies if a selection intersects with a given buffer range.
         * @param {Range} A {Range} to check against.
         * @returns {boolean} Returns a {Boolean}
         */
        intersectsBufferRange(bufferRange: Range): boolean;
        /**
         * Identifies if a selection intersects with another selection.
         * @param {Selection} A {Selection} to check against.
         * @returns {boolean} Returns a {Boolean}
         */
        intersectsWith(otherSelection: Selection): boolean;
        /**
         * Clears the selection, moving the marker to the head.
         * @param {Object} {Object} with the following keys:
         */
        clear(options?: Object): void;
        /**
         * Selects the text from the current cursor position to a given screen
         * position.
         * @param {Point} An instance of {Point}, with a given `row` and `column`. 
         */
        selectToScreenPosition(position: Point): void;
        /**
         * Selects the text from the current cursor position to a given buffer
         * position.
         * @param {Point} An instance of {Point}, with a given `row` and `column`. 
         */
        selectToBufferPosition(position: Point): void;
        /**
         * Selects the text one position right of the cursor.
         * @param {number} {Number} number of columns to select (default: 1) 
         */
        selectRight(columnCount?: number): void;
        /**
         * Selects the text one position left of the cursor.
         * @param {number} {Number} number of columns to select (default: 1) 
         */
        selectLeft(columnCount?: number): void;
        /**
         * Selects all the text one position above the cursor.
         * @param {number} {Number} number of rows to select (default: 1) 
         */
        selectUp(rowCount?: number): void;
        /**
         * Selects all the text one position below the cursor.
         * @param {number} {Number} number of rows to select (default: 1) 
         */
        selectDown(rowCount?: number): void;
        /**
         * Selects all the text from the current cursor position to the top of
         * the buffer. 
         */
        selectToTop(): void;
        /**
         * Selects all the text from the current cursor position to the bottom
         * of the buffer. 
         */
        selectToBottom(): void;
        /**
         * Selects all the text in the buffer. 
         */
        selectAll(): void;
        /**
         * Selects all the text from the current cursor position to the
         * beginning of the line. 
         */
        selectToBeginningOfLine(): void;
        /**
         * Selects all the text from the current cursor position to the first
         * character of the line. 
         */
        selectToFirstCharacterOfLine(): void;
        /**
         * Selects all the text from the current cursor position to the end of
         * the line. 
         */
        selectToEndOfLine(): void;
        /**
         * Selects all the text from the current cursor position to the
         * beginning of the word. 
         */
        selectToBeginningOfWord(): void;
        /**
         * Selects all the text from the current cursor position to the end of
         * the word. 
         */
        selectToEndOfWord(): void;
        /**
         * Selects all the text from the current cursor position to the
         * beginning of the next word. 
         */
        selectToBeginningOfNextWord(): void;
        /**
         * Selects text to the previous word boundary. 
         */
        selectToPreviousWordBoundary(): void;
        /**
         * Selects text to the next word boundary. 
         */
        selectToNextWordBoundary(): void;
        /**
         * Selects all the text from the current cursor position to the
         * beginning of the next paragraph. 
         */
        selectToBeginningOfNextParagraph(): void;
        /**
         * Selects all the text from the current cursor position to the
         * beginning of the previous paragraph. 
         */
        selectToBeginningOfPreviousParagraph(): void;
        /**
         * Modifies the selection to encompass the current word.
         * @returns {Range} Returns a {Range}.
         */
        selectWord(): Range;
        /**
         * Expands the newest selection to include the entire word on which
         * the cursors rests. 
         */
        expandOverWord(): void;
        /**
         * Selects an entire line in the buffer.
         * @param {number} The line {Number} to select (default: the row of the cursor). 
         */
        selectLine(row: number): void;
        /**
         * Expands the newest selection to include the entire line on which
         * the cursor currently rests.
         * 
         * It also includes the newline character. 
         */
        expandOverLine(): void;
        /**
         * Replaces text at the current selection.
         * @param {string} A {String} representing the text to add
         * @param {Object} {Object} with keys:
         */
        insertText(text: string, options?: Object): void;
        /**
         * Removes the first character before the selection if the selection
         * is empty otherwise it deletes the selection. 
         */
        backspace(): void;
        /**
         * Removes from the start of the selection to the beginning of the
         * current word if the selection is empty otherwise it deletes the selection. 
         */
        deleteToBeginningOfWord(): void;
        /**
         * Removes from the beginning of the line which the selection begins on
         * all the way through to the end of the selection. 
         */
        deleteToBeginningOfLine(): void;
        /**
         * Removes the selection or the next character after the start of the
         * selection if the selection is empty. 
         */
        delete(): void;
        /**
         * If the selection is empty, removes all text from the cursor to the
         * end of the line. If the cursor is already at the end of the line, it
         * removes the following newline. If the selection isn't empty, only deletes
         * the contents of the selection. 
         */
        deleteToEndOfLine(): void;
        /**
         * Removes the selection or all characters from the start of the
         * selection to the end of the current word if nothing is selected. 
         */
        deleteToEndOfWord(): void;
        /**
         * Removes only the selected text. 
         */
        deleteSelectedText(): void;
        /**
         * Removes the line at the beginning of the selection if the selection
         * is empty unless the selection spans multiple lines in which case all lines
         * are removed. 
         */
        deleteLine(): void;
        /**
         * Joins the current line with the one below it. Lines will
         * be separated by a single space.
         * 
         * If there selection spans more than one line, all the lines are joined together. 
         */
        joinLines(): void;
        /**
         * Removes one level of indent from the currently selected rows. 
         */
        outdentSelectedRows(): void;
        /**
         * Sets the indentation level of all selected rows to values suggested
         * by the relevant grammars. 
         */
        autoIndentSelectedRows(): void;
        /**
         * Wraps the selected lines in comments if they aren't currently part
         * of a comment.
         * 
         * Removes the comment if they are currently wrapped in a comment. 
         */
        toggleLineComments(): void;
        /**
         * Cuts the selection until the end of the line. 
         */
        cutToEndOfLine(): void;
        /**
         * Copies the selection to the clipboard and then deletes it.
         * @param {boolean} {Boolean} (default: false) See {::copy}
         * @param {boolean} {Boolean} (default: false) See {::copy} 
         */
        cut(maintainClipboard: boolean, fullLine: boolean): void;
        /**
         * Copies the current selection to the clipboard.
         * @param {boolean} {Boolean} if `true`, a specific metadata property is created to store each content copied to the clipboard. The clipboard `text` still contains the concatenation of the clipboard with the current selection. (default: false)
         * @param {boolean} {Boolean} if `true`, the copied text will always be pasted at the beginning of the line containing the cursor, regardless of the cursor's horizontal position. (default: false) 
         */
        copy(maintainClipboard: boolean, fullLine: boolean): void;
        /**
         * Creates a fold containing the current selection. 
         */
        fold(): void;
        /**
         * If the selection spans multiple rows, indent all of them. 
         */
        indentSelectedRows(): void;
        /**
         * Moves the selection down one row. 
         */
        addSelectionBelow(): void;
        /**
         * Moves the selection up one row. 
         */
        addSelectionAbove(): void;
        /**
         * Combines the given selection into this selection and then destroys
         * the given selection.
         * @param {Selection} A {Selection} to merge with.
         * @param {Object} {Object} options matching those found in {::setBufferRange}. 
         */
        merge(otherSelection: Selection, options?: Object): void;
        /**
         * Compare this selection's buffer range to another selection's buffer
         * range.
         * 
         * See {Range::compare} for more details.
         * @param {Selection} A {Selection} to compare against 
         */
        compare(otherSelection: Selection): void;
    }

    /**
     * A singleton instance of this class available via `atom.styles`,
     * which you can use to globally query and observe the set of active style
     * sheets. The `StyleManager` doesn't add any style elements to the DOM on its
     * own, but is instead subscribed to by individual `<atom-styles>` elements,
     * which clone and attach style elements in different contexts. 
     *
     * file: src/style-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/style-manager.coffee#L11
     */
    class StyleManager { 
        /**
         * Invoke `callback` for all current and future style elements.
         * @param {Function} {Function} that is called with style elements.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to cancel the
        subscription.
         */
        observeStyleElements(callback: Function): Disposable;
        /**
         * Invoke `callback` when a style element is added.
         * @param {Function} {Function} that is called with style elements.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to cancel the
        subscription.
         */
        onDidAddStyleElement(callback: Function): Disposable;
        /**
         * Invoke `callback` when a style element is removed.
         * @param {Function} {Function} that is called with style elements.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to cancel the
        subscription.
         */
        onDidRemoveStyleElement(callback: Function): Disposable;
        /**
         * Invoke `callback` when an existing style element is updated.
         * @param {Function} {Function} that is called with style elements.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to cancel the
        subscription.
         */
        onDidUpdateStyleElement(callback: Function): Disposable;
        /**
         * Get all loaded style elements. 
         */
        getStyleElements(): void;
        /**
         * Get the path of the user style sheet in `~/.atom`.
         * @returns {string} Returns a {String}.
         */
        getUserStyleSheetPath(): string;
    }

    /**
     * Run a node script in a separate process.
     * 
     * Used by the fuzzy-finder and [find in project](https://github.com/atom/atom/blob/master/src/scan-handler.coffee).
     * 
     * For a real-world example, see the [scan-handler](https://github.com/atom/atom/blob/master/src/scan-handler.coffee)
     * and the [instantiation of the task](https://github.com/atom/atom/blob/4a20f13162f65afc816b512ad7201e528c3443d7/src/project.coffee#L245).
     *
     * file: src/task.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/task.coffee#L40
     */
    class Task { 
        /**
         * A helper method to easily launch and run a task once.
         * @param {string} The {String} path to the CoffeeScript/JavaScript file which exports a single {Function} to execute.
         * @param  The arguments to pass to the exported function.
         * @returns {Task} Returns the created {Task}.
         */
        static once(taskPath: string, args: any): Task;

        /**
         * Creates a task. You should probably use {.once}
         * @param {string} The {String} path to the CoffeeScript/JavaScript file that exports a single {Function} to execute. 
         */
        constructor(taskPath: string);
        /**
         * Starts the task.
         * 
         * Throws an error if this task has already been terminated or if sending a
         * message to the child process fails.
         * @param  The arguments to pass to the function exported by this task's script.
         * @param {Function} A {Function} to call when the task completes. 
         */
        start(args: any, callback?: Function): void;
        /**
         * Send message to the task.
         * 
         * Throws an error if this task has already been terminated or if sending a
         * message to the child process fails.
         * @param  The message to send to the task. 
         */
        send(message: any): void;
        /**
         * Call a function when an event is emitted by the child process
         * @param {string} The {String} name of the event to handle.
         * @param {Function} The {Function} to call when the event is emitted.
         * @returns {Disposable} Returns a {Disposable} that can be used to stop listening for the event.
         */
        on(eventName: string, callback: Function): Disposable;
        /**
         * Forcefully stop the running task.
         * 
         * No more events are emitted once this method is called. 
         */
        terminate(): void;
    }

    /**
     * A mutable text container with undo/redo support and the ability to
     * annotate logical regions in the text. 
     *
     * file: src/text-buffer.coffee
     * srcUrl: https://github.com/atom/text-buffer/blob/v5.2.0/src/text-buffer.coffee#L20
     */
    class TextBuffer { 
        /**
         * Create a new buffer with the given params.
         * @param {Object} {Object} or {String} of text
         */
        constructor(params: Object);
        /**
         * Invoke the given callback synchronously _before_ the content of the
         * buffer changes.
         * 
         * Because observers are invoked synchronously, it's important not to perform
         * any expensive operations via this method.
         * @param {Function} {Function} to be called when the buffer changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillChange(callback: Function): Disposable;
        /**
         * Invoke the given callback synchronously when the content of the
         * buffer changes.
         * 
         * Because observers are invoked synchronously, it's important not to perform
         * any expensive operations via this method. Consider {::onDidStopChanging} to
         * delay expensive operations until after changes stop occurring.
         * @param {Function} {Function} to be called when the buffer changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChange(callback: Function): Disposable;
        /**
         * Invoke the given callback asynchronously following one or more
         * changes after {::getStoppedChangingDelay} milliseconds elapse without an
         * additional change.
         * 
         * This method can be used to perform potentially expensive operations that
         * don't need to be performed synchronously. If you need to run your callback
         * synchronously, use {::onDidChange} instead.
         * @param {Function} {Function} to be called when the buffer stops changing.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidStopChanging(callback: Function): Disposable;
        /**
         * Invoke the given callback when the in-memory contents of the
         * buffer become in conflict with the contents of the file on disk.
         * @param {Function} {Function} to be called when the buffer enters conflict.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidConflict(callback: Function): Disposable;
        /**
         * Invoke the given callback the value of {::isModified} changes.
         * @param {Function} {Function} to be called when {::isModified} changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeModified(callback: Function): Disposable;
        /**
         * Invoke the given callback when all marker `::onDidChange`
         * observers have been notified following a change to the buffer.
         * 
         * The order of events following a buffer change is as follows:
         * 
         * * The text of the buffer is changed
         * * All markers are updated accordingly, but their `::onDidChange` observers
         *   are not notified.
         * * `TextBuffer::onDidChange` observers are notified.
         * * `Marker::onDidChange` observers are notified.
         * * `TextBuffer::onDidUpdateMarkers` observers are notified.
         * 
         * Basically, this method gives you a way to take action after both a buffer
         * change and all associated marker changes.
         * @param {Function} {Function} to be called after markers are updated.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidUpdateMarkers(callback: Function): Disposable;
        /**
         * Invoke the given callback when a marker is created.
         * @param {Function} {Function} to be called when a marker is created.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidCreateMarker(callback: Function): Disposable;
        /**
         * Invoke the given callback when the value of {::getPath} changes.
         * @param {Function} {Function} to be called when the path changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangePath(callback: Function): Disposable;
        /**
         * Invoke the given callback when the value of {::getEncoding} changes.
         * @param {Function} {Function} to be called when the encoding changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeEncoding(callback: Function): Disposable;
        /**
         * Invoke the given callback before the buffer is saved to disk.
         * @param {Function} {Function} to be called before the buffer is saved.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillSave(callback: Function): Disposable;
        /**
         * Invoke the given callback after the buffer is saved to disk.
         * @param {Function} {Function} to be called after the buffer is saved.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidSave(callback: Function): Disposable;
        /**
         * Invoke the given callback after the file backing the buffer is
         * deleted.
         * @param {Function} {Function} to be called after the buffer is deleted.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDelete(callback: Function): Disposable;
        /**
         * Invoke the given callback before the buffer is reloaded from the
         * contents of its file on disk.
         * @param {Function} {Function} to be called before the buffer is reloaded.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillReload(callback: Function): Disposable;
        /**
         * Invoke the given callback after the buffer is reloaded from the
         * contents of its file on disk.
         * @param {Function} {Function} to be called after the buffer is reloaded.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidReload(callback: Function): Disposable;
        /**
         * Invoke the given callback when the buffer is destroyed.
         * @param {Function} {Function} to be called when the buffer is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * Invoke the given callback when there is an error in watching the
         * file.
         * @param {Function} {Function} callback
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillThrowWatchError(callback: Function): Disposable;
        /**
         * Get the number of milliseconds that will elapse without a change
         * before {::onDidStopChanging} observers are invoked following a change.
         * @returns {number} Returns a {Number}.
         */
        getStoppedChangingDelay(): number;
        /**
         * Determine if the in-memory contents of the buffer differ from its
         * contents on disk.
         * 
         * If the buffer is unsaved, always returns `true` unless the buffer is empty.
         * @returns {boolean} Returns a {Boolean}.
         */
        isModified(): boolean;
        /**
         * Determine if the in-memory contents of the buffer conflict with the
         * on-disk contents of its associated file.
         * @returns {boolean} Returns a {Boolean}.
         */
        isInConflict(): boolean;
        /**
         * Get the path of the associated file.
         * @returns {string} Returns a {String}.
         */
        getPath(): string;
        /**
         * Set the path for the buffer's associated file.
         * @param {string} A {String} representing the new file path 
         */
        setPath(filePath: string): void;
        /**
         * Sets the character set encoding for this buffer.
         * @param {string} The {String} encoding to use (default: 'utf8'). 
         */
        setEncoding(encoding: string): void;
        /**
         * @returns {string} Returns the {String} encoding of this buffer.
         */
        getEncoding(): string;
        /**
         * Get the path of the associated file.
         * @returns {string} Returns a {String}.
         */
        getUri(): string;
        /**
         * Determine whether the buffer is empty.
         * @returns {boolean} Returns a {Boolean}.
         */
        isEmpty(): boolean;
        /**
         * Get the entire text of the buffer.
         * @returns {string} Returns a {String}.
         */
        getText(): string;
        /**
         * Get the text in a range.
         * @param {Range} A {Range}
         * @returns {string} Returns a {String}
         */
        getTextInRange(range: Range): string;
        /**
         * Get the text of all lines in the buffer, without their line endings.
         * @returns {any[]} Returns an {Array} of {String}s.
         */
        getLines(): any[];
        /**
         * Get the text of the last line of the buffer, without its line
         * ending.
         * @returns {string} Returns a {String}.
         */
        getLastLine(): string;
        /**
         * Get the text of the line at the given row, without its line ending.
         * @param {number} A {Number} representing a 0-indexed row.
         * @returns {string} Returns a {String}.
         */
        lineForRow(row: number): string;
        /**
         * Get the line ending for the given 0-indexed row.
         * @param {number} A {Number} indicating the row.
         * @returns {string} Returns a {String}. The returned newline is represented as a literal string:
        `'\n'`, `'\r'`, `'\r\n'`, or `''` for the last line of the buffer, which
        doesn't end in a newline.
         */
        lineEndingForRow(row: number): string;
        /**
         * Get the length of the line for the given 0-indexed row, without its
         * line ending.
         * @param {number} A {Number} indicating the row.
         * @returns {number} Returns a {Number}.
         */
        lineLengthForRow(row: number): number;
        /**
         * Determine if the given row contains only whitespace.
         * @param {number} A {Number} representing a 0-indexed row.
         * @returns {boolean} Returns a {Boolean}.
         */
        isRowBlank(row: number): boolean;
        /**
         * Given a row, find the first preceding row that's not blank.
         * @param {number} A {Number} identifying the row to start checking at.
         * @returns {number} Returns a {Number} or `null` if there's no preceding non-blank row.
         */
        previousNonBlankRow(startRow: number): number;
        /**
         * Given a row, find the next row that's not blank.
         * @param {number} A {Number} identifying the row to start checking at.
         * @returns {number} Returns a {Number} or `null` if there's no next non-blank row.
         */
        nextNonBlankRow(startRow: number): number;
        /**
         * Replace the entire contents of the buffer with the given text.
         * @param {string} A {String}
         * @returns {Range} Returns a {Range} spanning the new buffer contents.
         */
        setText(text: string): Range;
        /**
         * Replace the current buffer contents by applying a diff based on the
         * given text.
         * @param {string} A {String} containing the new buffer contents. 
         */
        setTextViaDiff(text: string): void;
        /**
         * Set the text in the given range.
         * @param {Range} A {Range}
         * @param {string} A {String}
         * @param {Object} {Object}
         * @returns {Range} Returns the {Range} of the inserted text.
         */
        setTextInRange(range: Range, text: string, options?: Object): Range;
        /**
         * Insert text at the given position.
         * @param {Point} A {Point} representing the insertion location. The position is clipped before insertion.
         * @param {string} A {String} representing the text to insert.
         * @param {Object} {Object}
         * @returns {Range} Returns the {Range} of the inserted text.
         */
        insert(position: Point, text: string, options?: Object): Range;
        /**
         * Append text to the end of the buffer.
         * @param {string} A {String} representing the text text to append.
         * @param {Object} {Object}
         * @returns {Range} Returns the {Range} of the inserted text
         */
        append(text: string, options?: Object): Range;
        /**
         * Delete the text in the given range.
         * @param {Range} A {Range} in which to delete. The range is clipped before deleting.
         * @returns {Range} Returns an empty {Range} starting at the start of deleted range.
         */
        delete(range: Range): Range;
        /**
         * Delete the line associated with a specified row.
         * @param {number} A {Number} representing the 0-indexed row to delete.
         * @returns {Range} Returns the {Range} of the deleted text.
         */
        deleteRow(row: number): Range;
        /**
         * Delete the lines associated with the specified row range.
         * 
         * If the row range is out of bounds, it will be clipped. If the startRow is
         * greater than the end row, they will be reordered.
         * @param {number} A {Number} representing the first row to delete.
         * @param {number} A {Number} representing the last row to delete, inclusive.
         * @returns {Range} Returns the {Range} of the deleted text.
         */
        deleteRows(startRow: number, endRow: number): Range;
        /**
         * Create a marker with the given range. This marker will maintain
         * its logical location as the buffer is changed, so if you mark a particular
         * word, the marker will remain over that word even if the word's location in
         * the buffer changes.
         * @param {Range} A {Range} or range-compatible {Array}
         * @param  A hash of key-value pairs to associate with the marker. There are also reserved property names that have marker-specific meaning.
         * @returns {Marker} Returns a {Marker}.
         */
        markRange(range: Range, properties: any): Marker;
        /**
         * Create a marker at the given position with no tail.
         * @param {Point} {Point} or point-compatible {Array}
         * @param  This is the same as the `properties` parameter in {::markRange}
         * @returns {Marker} Returns a {Marker}.
         */
        markPosition(position: Point, properties: any): Marker;
        /**
         * Get all existing markers on the buffer.
         * @returns {any[]} Returns an {Array} of {Marker}s.
         */
        getMarkers(): any[];
        /**
         * Get an existing marker by its id.
         * @param {number} {Number} id of the marker to retrieve
         * @returns {Marker} Returns a {Marker}.
         */
        getMarker(id: number): Marker;
        /**
         * Find markers conforming to the given parameters.
         * 
         * Markers are sorted based on their position in the buffer. If two markers
         * start at the same position, the larger marker comes first.
         * @param  A hash of key-value pairs constraining the set of returned markers. You can query against custom marker properties by listing the desired key-value pairs here. In addition, the following keys are reserved and have special semantics:
         * @returns {any[]} Returns an {Array} of {Marker}s.
         */
        findMarkers(params: any): any[];
        /**
         * Get the number of markers in the buffer.
         * @returns {number} Returns a {Number}.
         */
        getMarkerCount(): number;
        /**
         * Undo the last operation. If a transaction is in progress, aborts it. 
         */
        undo(): void;
        /**
         * Redo the last operation 
         */
        redo(): void;
        /**
         * Batch multiple operations as a single undo/redo step.
         * 
         * Any group of operations that are logically grouped from the perspective of
         * undoing and redoing should be performed in a transaction. If you want to
         * abort the transaction, call {::abortTransaction} to terminate the function's
         * execution and revert any changes performed up to the abortion.
         * @param {number} The {Number} of milliseconds for which this transaction should be considered 'open for grouping' after it begins. If a transaction with a positive `groupingInterval` is committed while the previous transaction is still open for grouping, the two transactions are merged with respect to undo and redo.
         * @param {Function} A {Function} to call inside the transaction. 
         */
        transact(groupingInterval?: number, fn?: Function): void;
        /**
         * Clear the undo stack. 
         */
        clearUndoStack(): void;
        /**
         * Create a pointer to the current state of the buffer for use
         * with {::revertToCheckpoint} and {::groupChangesSinceCheckpoint}.
         * @returns  Returns a checkpoint value.
         */
        createCheckpoint(): any;
        /**
         * Revert the buffer to the state it was in when the given
         * checkpoint was created.
         * 
         * The redo stack will be empty following this operation, so changes since the
         * checkpoint will be lost. If the given checkpoint is no longer present in the
         * undo history, no changes will be made to the buffer and this method will
         * return `false`.
         * @returns {boolean} Returns a {Boolean} indicating whether the operation succeeded.
         */
        revertToCheckpoint(): boolean;
        /**
         * Group all changes since the given checkpoint into a single
         * transaction for purposes of undo/redo.
         * 
         * If the given checkpoint is no longer present in the undo history, no
         * grouping will be performed and this method will return `false`.
         * @returns {boolean} Returns a {Boolean} indicating whether the operation succeeded.
         */
        groupChangesSinceCheckpoint(): boolean;
        /**
         * Scan regular expression matches in the entire buffer, calling the
         * given iterator function on each match.
         * 
         * If you're programmatically modifying the results, you may want to try
         * {::backwardsScan} to avoid tripping over your own changes.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        scan(regex: RegExp, iterator: Function): void;
        /**
         * Scan regular expression matches in the entire buffer in reverse
         * order, calling the given iterator function on each match.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        backwardsScan(regex: RegExp, iterator: Function): void;
        /**
         * Scan regular expression matches in a given range , calling the given
         * iterator function on each match.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Range} A {Range} in which to search.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        scanInRange(regex: RegExp, range: Range, iterator: Function): void;
        /**
         * Scan regular expression matches in a given range in reverse order,
         * calling the given iterator function on each match.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Range} A {Range} in which to search.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        backwardsScanInRange(regex: RegExp, range: Range, iterator: Function): void;
        /**
         * Replace all regular expression matches in the entire buffer.
         * @param {RegExp} A {RegExp} representing the matches to be replaced.
         * @param {string} A {String} representing the text to replace each match.
         * @returns {number} Returns a {Number} representing the number of replacements made.
         */
        replace(regex: RegExp, replacementText: string): number;
        /**
         * Get the range spanning from `[0, 0]` to {::getEndPosition}.
         * @returns {Range} Returns a {Range}.
         */
        getRange(): Range;
        /**
         * Get the number of lines in the buffer.
         * @returns {number} Returns a {Number}.
         */
        getLineCount(): number;
        /**
         * Get the last 0-indexed row in the buffer.
         * @returns {number} Returns a {Number}.
         */
        getLastRow(): number;
        /**
         * Get the first position in the buffer, which is always `[0, 0]`.
         * @returns {Point} Returns a {Point}.
         */
        getFirstPosition(): Point;
        /**
         * Get the maximal position in the buffer, where new text would be
         * appended.
         * @returns {Point} Returns a {Point}.
         */
        getEndPosition(): Point;
        /**
         * Get the length of the buffer in characters.
         * @returns {number} Returns a {Number}.
         */
        getMaxCharacterIndex(): number;
        /**
         * Get the range for the given row
         * @param {number} A {Number} representing a 0-indexed row.
         * @param {boolean} A {Boolean} indicating whether or not to include the newline, which results in a range that extends to the start of the next line.
         * @returns {Range} Returns a {Range}.
         */
        rangeForRow(row: number, includeNewline: boolean): Range;
        /**
         * Convert a position in the buffer in row/column coordinates to an
         * absolute character offset, inclusive of line ending characters.
         * 
         * The position is clipped prior to translating.
         * @param {Point} A {Point}.
         * @returns {number} Returns a {Number}.
         */
        characterIndexForPosition(position: Point): number;
        /**
         * Convert an absolute character offset, inclusive of newlines, to a
         * position in the buffer in row/column coordinates.
         * 
         * The offset is clipped prior to translating.
         * @param {number} A {Number}.
         * @returns {Point} Returns a {Point}.
         */
        positionForCharacterIndex(offset: number): Point;
        /**
         * Clip the given range so it starts and ends at valid positions.
         * 
         * For example, the position `[1, 100]` is out of bounds if the line at row 1 is
         * only 10 characters long, and it would be clipped to `(1, 10)`.
         * @param {Range} A {Range} or range-compatible {Array} to clip.
         * @returns {Range} Returns the given {Range} if it is already in bounds, or a new clipped
        {Range} if the given range is out-of-bounds.
         */
        clipRange(range: Range): Range;
        /**
         * Clip the given point so it is at a valid position in the buffer.
         * 
         * For example, the position (1, 100) is out of bounds if the line at row 1 is
         * only 10 characters long, and it would be clipped to (1, 10)
         * @param {Point} A {Point} or point-compatible {Array}.
         * @returns {Point} Returns a new {Point} if the given position is invalid, otherwise returns
        the given position.
         */
        clipPosition(position: Point): Point;
        /**
         * Save the buffer. 
         */
        save(): void;
        /**
         * Save the buffer at a specific path.
         * @param  The path to save at. 
         */
        saveAs(filePath: any): void;
        /**
         * Reload the buffer's contents from disk.
         * 
         * Sets the buffer's content to the cached disk contents 
         */
        reload(): void;
    }

    /**
     * This class represents all essential editing state for a single
     * {TextBuffer}, including cursor and selection positions, folds, and soft wraps.
     * If you're manipulating the state of an editor, use this class. If you're
     * interested in the visual appearance of editors, use {TextEditorView} instead.
     * 
     * A single {TextBuffer} can belong to multiple editors. For example, if the
     * same file is open in two different panes, Atom creates a separate editor for
     * each pane. If the buffer is manipulated the changes are reflected in both
     * editors, but each maintains its own cursor position, folded lines, etc.
     * 
     * ## Accessing TextEditor Instances
     * 
     * The easiest way to get hold of `TextEditor` objects is by registering a callback
     * with `::observeTextEditors` on the `atom.workspace` global. Your callback will
     * then be called with all current editor instances and also when any editor is
     * created in the future.
     * 
     * ```coffee
     * atom.workspace.observeTextEditors (editor) ->
     *   editor.insertText('Hello World')
     * ```
     * 
     * ## Buffer vs. Screen Coordinates
     * 
     * Because editors support folds and soft-wrapping, the lines on screen don't
     * always match the lines in the buffer. For example, a long line that soft wraps
     * twice renders as three lines on screen, but only represents one line in the
     * buffer. Similarly, if rows 5-10 are folded, then row 6 on screen corresponds
     * to row 11 in the buffer.
     * 
     * Your choice of coordinates systems will depend on what you're trying to
     * achieve. For example, if you're writing a command that jumps the cursor up or
     * down by 10 lines, you'll want to use screen coordinates because the user
     * probably wants to skip lines *on screen*. However, if you're writing a package
     * that jumps between method definitions, you'll want to work in buffer
     * coordinates.
     * 
     * **When in doubt, just default to buffer coordinates**, then experiment with
     * soft wraps and folds to ensure your code interacts with them correctly. 
     *
     * file: src/text-editor.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/text-editor.coffee#L56
     */
    class TextEditor { 
        /**
         * Calls your `callback` when the buffer's title has changed.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeTitle(callback: Function): Disposable;
        /**
         * Calls your `callback` when the buffer's path, and therefore title, has changed.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangePath(callback: Function): Disposable;
        /**
         * Invoke the given callback synchronously when the content of the
         * buffer changes.
         * 
         * Because observers are invoked synchronously, it's important not to perform
         * any expensive operations via this method. Consider {::onDidStopChanging} to
         * delay expensive operations until after changes stop occurring.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChange(callback: Function): Disposable;
        /**
         * Invoke `callback` when the buffer's contents change. It is
         * emit asynchronously 300ms after the last buffer change. This is a good place
         * to handle changes to the buffer without compromising typing performance.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidStopChanging(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Cursor} is moved. If there are
         * multiple cursors, your callback will be called for each cursor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeCursorPosition(callback: Function): Disposable;
        /**
         * Calls your `callback` when a selection's screen range changes.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeSelectionRange(callback: Function): Disposable;
        /**
         * Calls your `callback` when soft wrap was enabled or disabled.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeSoftWrapped(callback: Function): Disposable;
        /**
         * Calls your `callback` when the buffer's encoding has changed.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeEncoding(callback: Function): Disposable;
        /**
         * Calls your `callback` when the grammar that interprets and
         * colorizes the text has been changed. Immediately calls your callback with
         * the current grammar.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeGrammar(callback: Function): Disposable;
        /**
         * Calls your `callback` when the grammar that interprets and
         * colorizes the text has been changed.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeGrammar(callback: Function): Disposable;
        /**
         * Calls your `callback` when the result of {::isModified} changes.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeModified(callback: Function): Disposable;
        /**
         * Calls your `callback` when the buffer's underlying file changes on
         * disk at a moment when the result of {::isModified} is true.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidConflict(callback: Function): Disposable;
        /**
         * Calls your `callback` before text has been inserted.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onWillInsertText(callback: Function): Disposable;
        /**
         * Calls your `callback` adter text has been inserted.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidInsertText(callback: Function): Disposable;
        /**
         * Invoke the given callback after the buffer is saved to disk.
         * @param {Function} {Function} to be called after the buffer is saved.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidSave(callback: Function): Disposable;
        /**
         * Invoke the given callback when the editor is destroyed.
         * @param {Function} {Function} to be called when the editor is destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroy(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Cursor} is added to the editor.
         * Immediately calls your callback for each existing cursor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeCursors(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Cursor} is added to the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddCursor(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Cursor} is removed from the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidRemoveCursor(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Selection} is added to the editor.
         * Immediately calls your callback for each existing selection.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeSelections(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Selection} is added to the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddSelection(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Selection} is removed from the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidRemoveSelection(callback: Function): Disposable;
        /**
         * Calls your `callback` with each {Decoration} added to the editor.
         * Calls your `callback` immediately for any existing decorations.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeDecorations(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Decoration} is added to the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddDecoration(callback: Function): Disposable;
        /**
         * Calls your `callback` when a {Decoration} is removed from the editor.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidRemoveDecoration(callback: Function): Disposable;
        /**
         * Calls your `callback` when the placeholder text is changed.
         * @param {Function} {Function}
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangePlaceholderText(callback: Function): Disposable;
        /**
         * Retrieves the current {TextBuffer}. 
         */
        getBuffer(): void;
        /**
         * Get the editor's title for display in other parts of the
         * UI such as the tabs.
         * 
         * If the editor's buffer is saved, its title is the file name. If it is
         * unsaved, its title is "untitled".
         * @returns {string} Returns a {String}.
         */
        getTitle(): string;
        /**
         * Get the editor's long title for display in other parts of the UI
         * such as the window title.
         * 
         * If the editor's buffer is saved, its long title is formatted as
         * "<filename> - <directory>". If it is unsaved, its title is "untitled"
         * @returns {string} Returns a {String}.
         */
        getLongTitle(): string;
        /**
         * @returns {string} Returns the {String} path of this editor's text buffer.
         */
        getPath(): string;
        /**
         * @returns {string} Returns the {String} character set encoding of this editor's text
        buffer.
         */
        getEncoding(): string;
        /**
         * Set the character set encoding to use in this editor's text
         * buffer.
         * @param {string} The {String} character set encoding name such as 'utf8' 
         */
        setEncoding(encoding: string): void;
        /**
         * @returns {boolean} Returns {Boolean} `true` if this editor has been modified.
         */
        isModified(): boolean;
        /**
         * @returns {boolean} Returns {Boolean} `true` if this editor has no content.
         */
        isEmpty(): boolean;
        /**
         * Saves the editor's text buffer.
         * 
         * See {TextBuffer::save} for more details. 
         */
        save(): void;
        /**
         * Saves the editor's text buffer as the given path.
         * 
         * See {TextBuffer::saveAs} for more details.
         * @param {string} A {String} path. 
         */
        saveAs(filePath: string): void;
        /**
         * @returns {string} Returns a {String} representing the entire contents of the editor.
         */
        getText(): string;
        /**
         * Get the text in the given {Range} in buffer coordinates.
         * @param {Range} A {Range} or range-compatible {Array}.
         * @returns {string} Returns a {String}.
         */
        getTextInBufferRange(range: Range): string;
        /**
         * @returns {number} Returns a {Number} representing the number of lines in the buffer.
         */
        getLineCount(): number;
        /**
         * @returns {number} Returns a {Number} representing the number of screen lines in the
        editor. This accounts for folds.
         */
        getScreenLineCount(): number;
        /**
         * @returns {number} Returns a {Number} representing the last zero-indexed buffer row
        number of the editor.
         */
        getLastBufferRow(): number;
        /**
         * @returns {number} Returns a {Number} representing the last zero-indexed screen row
        number of the editor.
         */
        getLastScreenRow(): number;
        /**
         * @param {number} A {Number} representing a zero-indexed buffer row. 
         * @returns {string} Returns a {String} representing the contents of the line at the
        given buffer row.
         */
        lineTextForBufferRow(bufferRow: number): string;
        /**
         * @param {number} A {Number} representing a zero-indexed screen row. 
         * @returns {string} Returns a {String} representing the contents of the line at the
        given screen row.
         */
        lineTextForScreenRow(screenRow: number): string;
        /**
         * Get the {Range} of the paragraph surrounding the most recently added
         * cursor.
         * @returns {Range} Returns a {Range}.
         */
        getCurrentParagraphBufferRange(): Range;
        /**
         * Replaces the entire contents of the buffer with the given {String}. 
         */
        setText(): void;
        /**
         * Set the text in the given {Range} in buffer coordinates.
         * @param {Range} A {Range} or range-compatible {Array}.
         * @param {string} A {String}
         * @param {Object} {Object}
         * @returns {Range} Returns the {Range} of the newly-inserted text.
         */
        setTextInBufferRange(range: Range, text: string, options?: Object): Range;
        /**
         * For each selection, replace the selected text with the given text.
         * @param {string} A {String} representing the text to insert.
         * @param  See {Selection::insertText}.
         * @returns {Range} Returns a {Range} when the text has been inserted
         * @returns {boolean} Returns a {Bool} false when the text has not been inserted
         */
        insertText(text: string, options?: any): Range | boolean;
        /**
         * For each selection, replace the selected text with a newline. 
         */
        insertNewline(): void;
        /**
         * For each selection, if the selection is empty, delete the character
         * following the cursor. Otherwise delete the selected text. 
         */
        delete(): void;
        /**
         * For each selection, if the selection is empty, delete the character
         * preceding the cursor. Otherwise delete the selected text. 
         */
        backspace(): void;
        /**
         * Mutate the text of all the selections in a single transaction.
         * 
         * All the changes made inside the given {Function} can be reverted with a
         * single call to {::undo}.
         * @param {Function} A {Function} that will be called once for each {Selection}. The first    argument will be a {Selection} and the second argument will be the    {Number} index of that selection. 
         */
        mutateSelectedText(fn: Function): void;
        /**
         * For each selection, transpose the selected text.
         * 
         * If the selection is empty, the characters preceding and following the cursor
         * are swapped. Otherwise, the selected characters are reversed. 
         */
        transpose(): void;
        /**
         * Convert the selected text to upper case.
         * 
         * For each selection, if the selection is empty, converts the containing word
         * to upper case. Otherwise convert the selected text to upper case. 
         */
        upperCase(): void;
        /**
         * Convert the selected text to lower case.
         * 
         * For each selection, if the selection is empty, converts the containing word
         * to upper case. Otherwise convert the selected text to upper case. 
         */
        lowerCase(): void;
        /**
         * Toggle line comments for rows intersecting selections.
         * 
         * If the current grammar doesn't support comments, does nothing. 
         */
        toggleLineCommentsInSelection(): void;
        /**
         * For each cursor, insert a newline at beginning the following line. 
         */
        insertNewlineBelow(): void;
        /**
         * For each cursor, insert a newline at the end of the preceding line. 
         */
        insertNewlineAbove(): void;
        /**
         * For each selection, if the selection is empty, delete all characters
         * of the containing word that precede the cursor. Otherwise delete the
         * selected text. 
         */
        deleteToBeginningOfWord(): void;
        /**
         * For each selection, if the selection is empty, delete all characters
         * of the containing line that precede the cursor. Otherwise delete the
         * selected text. 
         */
        deleteToBeginningOfLine(): void;
        /**
         * For each selection, if the selection is not empty, deletes the
         * selection; otherwise, deletes all characters of the containing line
         * following the cursor. If the cursor is already at the end of the line,
         * deletes the following newline. 
         */
        deleteToEndOfLine(): void;
        /**
         * For each selection, if the selection is empty, delete all characters
         * of the containing word following the cursor. Otherwise delete the selected
         * text. 
         */
        deleteToEndOfWord(): void;
        /**
         * Delete all lines intersecting selections. 
         */
        deleteLine(): void;
        /**
         * Undo the last change. 
         */
        undo(): void;
        /**
         * Redo the last change. 
         */
        redo(): void;
        /**
         * Batch multiple operations as a single undo/redo step.
         * 
         * Any group of operations that are logically grouped from the perspective of
         * undoing and redoing should be performed in a transaction. If you want to
         * abort the transaction, call {::abortTransaction} to terminate the function's
         * execution and revert any changes performed up to the abortion.
         * @param {number} The {Number} of milliseconds for which this transaction should be considered 'groupable' after it begins. If a transaction with a positive `groupingInterval` is committed while the previous transaction is still 'groupable', the two transactions are merged with respect to undo and redo.
         * @param {Function} A {Function} to call inside the transaction. 
         */
        transact(groupingInterval?: number, fn?: Function): void;
        /**
         * Abort an open transaction, undoing any operations performed so far
         * within the transaction. 
         */
        abortTransaction(): void;
        /**
         * Create a pointer to the current state of the buffer for use
         * with {::revertToCheckpoint} and {::groupChangesSinceCheckpoint}.
         * @returns  Returns a checkpoint value.
         */
        createCheckpoint(): any;
        /**
         * Revert the buffer to the state it was in when the given
         * checkpoint was created.
         * 
         * The redo stack will be empty following this operation, so changes since the
         * checkpoint will be lost. If the given checkpoint is no longer present in the
         * undo history, no changes will be made to the buffer and this method will
         * return `false`.
         * @returns {boolean} Returns a {Boolean} indicating whether the operation succeeded.
         */
        revertToCheckpoint(): boolean;
        /**
         * Group all changes since the given checkpoint into a single
         * transaction for purposes of undo/redo.
         * 
         * If the given checkpoint is no longer present in the undo history, no
         * grouping will be performed and this method will return `false`.
         * @returns {boolean} Returns a {Boolean} indicating whether the operation succeeded.
         */
        groupChangesSinceCheckpoint(): boolean;
        /**
         * Convert a position in buffer-coordinates to screen-coordinates.
         * 
         * The position is clipped via {::clipBufferPosition} prior to the conversion.
         * The position is also clipped via {::clipScreenPosition} following the
         * conversion, which only makes a difference when `options` are supplied.
         * @param {Point} A {Point} or {Array} of [row, column].
         * @param  An options hash for {::clipScreenPosition}.
         * @returns {Point} Returns a {Point}.
         */
        screenPositionForBufferPosition(bufferPosition: Point, options?: any): Point;
        /**
         * Convert a position in screen-coordinates to buffer-coordinates.
         * 
         * The position is clipped via {::clipScreenPosition} prior to the conversion.
         * @param {Point} A {Point} or {Array} of [row, column].
         * @param  An options hash for {::clipScreenPosition}.
         * @returns {Point} Returns a {Point}.
         */
        bufferPositionForScreenPosition(bufferPosition: Point, options?: any): Point;
        /**
         * Convert a range in buffer-coordinates to screen-coordinates.
         * @param {Range} {Range} in buffer coordinates to translate into screen coordinates.
         * @returns {Range} Returns a {Range}.
         */
        screenRangeForBufferRange(bufferRange: Range): Range;
        /**
         * Convert a range in screen-coordinates to buffer-coordinates.
         * @param {Range} {Range} in screen coordinates to translate into buffer coordinates.
         * @returns {Range} Returns a {Range}.
         */
        bufferRangeForScreenRange(screenRange: Range): Range;
        /**
         * Clip the given {Point} to a valid position in the buffer.
         * 
         * If the given {Point} describes a position that is actually reachable by the
         * cursor based on the current contents of the buffer, it is returned
         * unchanged. If the {Point} does not describe a valid position, the closest
         * valid position is returned instead.
         * @param {Point} The {Point} representing the position to clip.
         * @returns {Point} Returns a {Point}.
         */
        clipBufferPosition(bufferPosition: Point): Point;
        /**
         * Clip the start and end of the given range to valid positions in the
         * buffer. See {::clipBufferPosition} for more information.
         * @param {Range} The {Range} to clip.
         * @returns {Range} Returns a {Range}.
         */
        clipBufferRange(range: Range): Range;
        /**
         * Clip the given {Point} to a valid position on screen.
         * 
         * If the given {Point} describes a position that is actually reachable by the
         * cursor based on the current contents of the screen, it is returned
         * unchanged. If the {Point} does not describe a valid position, the closest
         * valid position is returned instead.
         * @param {Point} The {Point} representing the position to clip.
         * @param {Object} {Object}
         * @returns {Point} Returns a {Point}.
         */
        clipScreenPosition(screenPosition: Point, options?: Object): Point;
        /**
         * Clip the start and end of the given range to valid positions on screen.
         * See {::clipScreenPosition} for more information.
         * @param {Range} The {Range} to clip.
         * @param {Range} See {::clipScreenPosition} `options`. Returns a {Range}. 
         */
        clipScreenRange(range: Range, options?: Range): void;
        /**
         * Adds a decoration that tracks a {Marker}. When the marker moves,
         * is invalidated, or is destroyed, the decoration will be updated to reflect
         * the marker's state.
         * 
         * There are three types of supported decorations:
         * 
         * * __line__: Adds your CSS `class` to the line nodes within the range
         *     marked by the marker
         * * __gutter__: Adds your CSS `class` to the line number nodes within the
         *     range marked by the marker
         * * __highlight__: Adds a new highlight div to the editor surrounding the
         *     range marked by the marker. When the user selects text, the selection is
         *     visualized with a highlight decoration internally. The structure of this
         *     highlight will be
         *   ```html
         *     <div class="highlight <your-class>">
         *       <!-- Will be one region for each row in the range. Spans 2 lines? There will be 2 regions. -->
         *       <div class="region"></div>
         *     </div>
         *   ```
         * @param {Marker} A {Marker} you want this decoration to follow.
         * @param {Object} An {Object} representing the decoration e.g. `{type: 'line-number', class: 'linter-error'}`
         * @returns {Decoration} Returns a {Decoration} object
         */
        decorateMarker(marker: Marker, decorationParams: Object): Decoration;
        /**
         * Get all the decorations within a screen row range.
         * @param {number} the {Number} beginning screen row
         * @param {number} the {Number} end screen row (inclusive)
         * @returns {Object} Returns an {Object} of decorations in the form
         `{1: [{id: 10, type: 'line-number', class: 'someclass'}], 2: ...}`
          where the keys are {Marker} IDs, and the values are an array of decoration
          params objects attached to the marker.
         * @returns  Returns an empty object when no decorations are found
         */
        decorationsForScreenRowRange(startScreenRow: number, endScreenRow: number): Object;
        /**
         * Get all decorations.
         * @param {Object} An {Object} containing key value pairs that the returned decorations' properties must match.
         * @returns {any[]} Returns an {Array} of {Decoration}s.
         */
        getDecorations(propertyFilter?: Object): any[];
        /**
         * Get all decorations of type 'line'.
         * @param {Object} An {Object} containing key value pairs that the returned decorations' properties must match.
         * @returns {any[]} Returns an {Array} of {Decoration}s.
         */
        getLineDecorations(propertyFilter?: Object): any[];
        /**
         * Get all decorations of type 'line-number'.
         * @param {Object} An {Object} containing key value pairs that the returned decorations' properties must match.
         * @returns {any[]} Returns an {Array} of {Decoration}s.
         */
        getLineNumberDecorations(propertyFilter?: Object): any[];
        /**
         * Get all decorations of type 'highlight'.
         * @param {Object} An {Object} containing key value pairs that the returned decorations' properties must match.
         * @returns {any[]} Returns an {Array} of {Decoration}s.
         */
        getHighlightDecorations(propertyFilter?: Object): any[];
        /**
         * Get all decorations of type 'overlay'.
         * @param {Object} An {Object} containing key value pairs that the returned decorations' properties must match.
         * @returns {any[]} Returns an {Array} of {Decoration}s.
         */
        getOverlayDecorations(propertyFilter?: Object): any[];
        /**
         * Create a marker with the given range in buffer coordinates. This
         * marker will maintain its logical location as the buffer is changed, so if
         * you mark a particular word, the marker will remain over that word even if
         * the word's location in the buffer changes.
         * @param {Range} A {Range} or range-compatible {Array}
         * @param  A hash of key-value pairs to associate with the marker. There are also reserved property names that have marker-specific meaning.
         * @returns {Marker} Returns a {Marker}.
         */
        markBufferRange(range: Range, properties: any): Marker;
        /**
         * Create a marker with the given range in screen coordinates. This
         * marker will maintain its logical location as the buffer is changed, so if
         * you mark a particular word, the marker will remain over that word even if
         * the word's location in the buffer changes.
         * @param {Range} A {Range} or range-compatible {Array}
         * @param  A hash of key-value pairs to associate with the marker. There are also reserved property names that have marker-specific meaning.
         * @returns {Marker} Returns a {Marker}.
         */
        markScreenRange(range: Range, properties: any): Marker;
        /**
         * Mark the given position in buffer coordinates.
         * @param {Point} A {Point} or {Array} of `[row, column]`.
         * @param  See {TextBuffer::markRange}.
         * @returns {Marker} Returns a {Marker}.
         */
        markBufferPosition(position: Point, options?: any): Marker;
        /**
         * Mark the given position in screen coordinates.
         * @param {Point} A {Point} or {Array} of `[row, column]`.
         * @param  See {TextBuffer::markRange}.
         * @returns {Marker} Returns a {Marker}.
         */
        markScreenPosition(position: Point, options?: any): Marker;
        /**
         * Find all {Marker}s that match the given properties.
         * 
         * This method finds markers based on the given properties. Markers can be
         * associated with custom properties that will be compared with basic equality.
         * In addition, there are several special properties that will be compared
         * with the range of the markers rather than their properties.
         * @param {Object} An {Object} containing properties that each returned marker must satisfy. Markers can be associated with custom properties, which are compared with basic equality. In addition, several reserved properties can be used to filter markers based on their current range:
         */
        findMarkers(properties: Object): void;
        /**
         * Get the {Marker} for the given marker id.
         * @param {number} {Number} id of the marker 
         */
        getMarker(id: number): void;
        /**
         * Get all {Marker}s. Consider using {::findMarkers} 
         */
        getMarkers(): void;
        /**
         * Get the number of markers in this editor's buffer.
         * @returns {number} Returns a {Number}.
         */
        getMarkerCount(): number;
        /**
         * Get the position of the most recently added cursor in buffer
         * coordinates.
         * @returns {Point} Returns a {Point}
         */
        getCursorBufferPosition(): Point;
        /**
         * Get the position of all the cursor positions in buffer coordinates.
         * @returns {any[]} Returns {Array} of {Point}s in the order they were added
         */
        getCursorBufferPositions(): any[];
        /**
         * Move the cursor to the given position in buffer coordinates.
         * 
         * If there are multiple cursors, they will be consolidated to a single cursor.
         * @param {Point} A {Point} or {Array} of `[row, column]`
         * @param {Object} An {Object} combining options for {::clipScreenPosition} with:
         */
        setCursorBufferPosition(position: Point, options?: Object): void;
        /**
         * Get the position of the most recently added cursor in screen
         * coordinates.
         * @returns {Point} Returns a {Point}.
         */
        getCursorScreenPosition(): Point;
        /**
         * Get the position of all the cursor positions in screen coordinates.
         * @returns {any[]} Returns {Array} of {Point}s in the order the cursors were added
         */
        getCursorScreenPositions(): any[];
        /**
         * Move the cursor to the given position in screen coordinates.
         * 
         * If there are multiple cursors, they will be consolidated to a single cursor.
         * @param {Point} A {Point} or {Array} of `[row, column]`
         * @param {Object} An {Object} combining options for {::clipScreenPosition} with:
         */
        setCursorScreenPosition(position: Point, options?: Object): void;
        /**
         * Add a cursor at the given position in buffer coordinates.
         * @param {Point} A {Point} or {Array} of `[row, column]`
         * @returns {Cursor} Returns a {Cursor}.
         */
        addCursorAtBufferPosition(bufferPosition: Point): Cursor;
        /**
         * Add a cursor at the position in screen coordinates.
         * @param {Point} A {Point} or {Array} of `[row, column]`
         * @returns {Cursor} Returns a {Cursor}.
         */
        addCursorAtScreenPosition(screenPosition: Point): Cursor;
        /**
         * @returns {boolean} Returns {Boolean} indicating whether or not there are multiple cursors.
         */
        hasMultipleCursors(): boolean;
        /**
         * Move every cursor up one row in screen coordinates.
         * @param {number} {Number} number of lines to move 
         */
        moveUp(lineCount?: number): void;
        /**
         * Move every cursor down one row in screen coordinates.
         * @param {number} {Number} number of lines to move 
         */
        moveDown(lineCount?: number): void;
        /**
         * Move every cursor left one column.
         * @param {number} {Number} number of columns to move (default: 1) 
         */
        moveLeft(columnCount?: number): void;
        /**
         * Move every cursor right one column.
         * @param {number} {Number} number of columns to move (default: 1) 
         */
        moveRight(columnCount?: number): void;
        /**
         * Move every cursor to the beginning of its line in buffer coordinates. 
         */
        moveToBeginningOfLine(): void;
        /**
         * Move every cursor to the beginning of its line in screen coordinates. 
         */
        moveToBeginningOfScreenLine(): void;
        /**
         * Move every cursor to the first non-whitespace character of its line. 
         */
        moveToFirstCharacterOfLine(): void;
        /**
         * Move every cursor to the end of its line in buffer coordinates. 
         */
        moveToEndOfLine(): void;
        /**
         * Move every cursor to the end of its line in screen coordinates. 
         */
        moveToEndOfScreenLine(): void;
        /**
         * Move every cursor to the beginning of its surrounding word. 
         */
        moveToBeginningOfWord(): void;
        /**
         * Move every cursor to the end of its surrounding word. 
         */
        moveToEndOfWord(): void;
        /**
         * Move every cursor to the top of the buffer.
         * 
         * If there are multiple cursors, they will be merged into a single cursor. 
         */
        moveToTop(): void;
        /**
         * Move every cursor to the bottom of the buffer.
         * 
         * If there are multiple cursors, they will be merged into a single cursor. 
         */
        moveToBottom(): void;
        /**
         * Move every cursor to the beginning of the next word. 
         */
        moveToBeginningOfNextWord(): void;
        /**
         * Move every cursor to the previous word boundary. 
         */
        moveToPreviousWordBoundary(): void;
        /**
         * Move every cursor to the next word boundary. 
         */
        moveToNextWordBoundary(): void;
        /**
         * Move every cursor to the beginning of the next paragraph. 
         */
        moveToBeginningOfNextParagraph(): void;
        /**
         * Move every cursor to the beginning of the previous paragraph. 
         */
        moveToBeginningOfPreviousParagraph(): void;
        /**
         * @returns {Cursor} Returns the most recently added {Cursor}
         */
        getLastCursor(): Cursor;
        /**
         * @param  See {Cursor::getBeginningOfCurrentWordBufferPosition}. 
         * @returns  Returns the word surrounding the most recently added cursor.
         */
        getWordUnderCursor(options?: any): any;
        /**
         * Get an Array of all {Cursor}s. 
         */
        getCursors(): void;
        /**
         * Get all {Cursors}s, ordered by their position in the buffer
         * instead of the order in which they were added.
         * @returns {any[]} Returns an {Array} of {Selection}s.
         */
        getCursorsOrderedByBufferPosition(): any[];
        /**
         * Get the selected text of the most recently added selection.
         * @returns {string} Returns a {String}.
         */
        getSelectedText(): string;
        /**
         * Get the {Range} of the most recently added selection in buffer
         * coordinates.
         * @returns {Range} Returns a {Range}.
         */
        getSelectedBufferRange(): Range;
        /**
         * Get the {Range}s of all selections in buffer coordinates.
         * 
         * The ranges are sorted by when the selections were added. Most recent at the end.
         * @returns {any[]} Returns an {Array} of {Range}s.
         */
        getSelectedBufferRanges(): any[];
        /**
         * Set the selected range in buffer coordinates. If there are multiple
         * selections, they are reduced to a single selection with the given range.
         * @param {Range} A {Range} or range-compatible {Array}.
         * @param {Object} An options {Object}:
         */
        setSelectedBufferRange(bufferRange: Range, options?: Object): void;
        /**
         * Set the selected ranges in buffer coordinates. If there are multiple
         * selections, they are replaced by new selections with the given ranges.
         * @param {any[]} An {Array} of {Range}s or range-compatible {Array}s.
         * @param {Object} An options {Object}:
         */
        setSelectedBufferRanges(bufferRanges: any[], options?: Object): void;
        /**
         * Get the {Range} of the most recently added selection in screen
         * coordinates.
         * @returns {Range} Returns a {Range}.
         */
        getSelectedScreenRange(): Range;
        /**
         * Get the {Range}s of all selections in screen coordinates.
         * 
         * The ranges are sorted by when the selections were added. Most recent at the end.
         * @returns {any[]} Returns an {Array} of {Range}s.
         */
        getSelectedScreenRanges(): any[];
        /**
         * Set the selected range in screen coordinates. If there are multiple
         * selections, they are reduced to a single selection with the given range.
         * @param {Range} A {Range} or range-compatible {Array}.
         * @param {Object} An options {Object}:
         */
        setSelectedScreenRange(screenRange: Range, options?: Object): void;
        /**
         * Set the selected ranges in screen coordinates. If there are multiple
         * selections, they are replaced by new selections with the given ranges.
         * @param {any[]} An {Array} of {Range}s or range-compatible {Array}s.
         * @param {Object} An options {Object}:
         */
        setSelectedScreenRanges(screenRanges: any[], options?: Object): void;
        /**
         * Add a selection for the given range in buffer coordinates.
         * @param {Range} A {Range}
         * @param {Object} An options {Object}:
         * @returns {Selection} Returns the added {Selection}.
         */
        addSelectionForBufferRange(bufferRange: Range, options?: Object): Selection;
        /**
         * Add a selection for the given range in screen coordinates.
         * @param {Range} A {Range}
         * @param {Object} An options {Object}:
         * @returns {Selection} Returns the added {Selection}.
         */
        addSelectionForScreenRange(screenRange: Range, options?: Object): Selection;
        /**
         * Select from the current cursor position to the given position in
         * buffer coordinates.
         * 
         * This method may merge selections that end up intesecting.
         * @param {Point} An instance of {Point}, with a given `row` and `column`. 
         */
        selectToBufferPosition(position: Point): void;
        /**
         * Select from the current cursor position to the given position in
         * screen coordinates.
         * 
         * This method may merge selections that end up intesecting.
         * @param {Point} An instance of {Point}, with a given `row` and `column`. 
         */
        selectToScreenPosition(position: Point): void;
        /**
         * Move the cursor of each selection one character upward while
         * preserving the selection's tail position.
         * 
         * This method may merge selections that end up intesecting. 
         * @param {number} {Number} number of rows to select (default: 1)
         */
        selectUp(rowCount?: number): void;
        /**
         * Move the cursor of each selection one character downward while
         * preserving the selection's tail position.
         * 
         * This method may merge selections that end up intesecting. 
         * @param {number} {Number} number of rows to select (default: 1)
         */
        selectDown(rowCount?: number): void;
        /**
         * Move the cursor of each selection one character leftward while
         * preserving the selection's tail position.
         * 
         * This method may merge selections that end up intesecting. 
         * @param {number} {Number} number of columns to select (default: 1)
         */
        selectLeft(columnCount?: number): void;
        /**
         * Move the cursor of each selection one character rightward while
         * preserving the selection's tail position.
         * 
         * This method may merge selections that end up intesecting. 
         * @param {number} {Number} number of columns to select (default: 1)
         */
        selectRight(columnCount?: number): void;
        /**
         * Select from the top of the buffer to the end of the last selection
         * in the buffer.
         * 
         * This method merges multiple selections into a single selection. 
         */
        selectToTop(): void;
        /**
         * Selects from the top of the first selection in the buffer to the end
         * of the buffer.
         * 
         * This method merges multiple selections into a single selection. 
         */
        selectToBottom(): void;
        /**
         * Select all text in the buffer.
         * 
         * This method merges multiple selections into a single selection. 
         */
        selectAll(): void;
        /**
         * Move the cursor of each selection to the beginning of its line
         * while preserving the selection's tail position.
         * 
         * This method may merge selections that end up intesecting. 
         */
        selectToBeginningOfLine(): void;
        /**
         * Move the cursor of each selection to the first non-whitespace
         * character of its line while preserving the selection's tail position. If the
         * cursor is already on the first character of the line, move it to the
         * beginning of the line.
         * 
         * This method may merge selections that end up intersecting. 
         */
        selectToFirstCharacterOfLine(): void;
        /**
         * Move the cursor of each selection to the end of its line while
         * preserving the selection's tail position.
         * 
         * This method may merge selections that end up intersecting. 
         */
        selectToEndOfLine(): void;
        /**
         * Expand selections to the beginning of their containing word.
         * 
         * Operates on all selections. Moves the cursor to the beginning of the
         * containing word while preserving the selection's tail position. 
         */
        selectToBeginningOfWord(): void;
        /**
         * Expand selections to the end of their containing word.
         * 
         * Operates on all selections. Moves the cursor to the end of the containing
         * word while preserving the selection's tail position. 
         */
        selectToEndOfWord(): void;
        /**
         * For each cursor, select the containing line.
         * 
         * This method merges selections on successive lines. 
         */
        selectLinesContainingCursors(): void;
        /**
         * Select the word surrounding each cursor. 
         */
        selectWordsContainingCursors(): void;
        /**
         * For each selection, move its cursor to the preceding word boundary
         * while maintaining the selection's tail position.
         * 
         * This method may merge selections that end up intersecting. 
         */
        selectToPreviousWordBoundary(): void;
        /**
         * For each selection, move its cursor to the next word boundary while
         * maintaining the selection's tail position.
         * 
         * This method may merge selections that end up intersecting. 
         */
        selectToNextWordBoundary(): void;
        /**
         * Expand selections to the beginning of the next word.
         * 
         * Operates on all selections. Moves the cursor to the beginning of the next
         * word while preserving the selection's tail position. 
         */
        selectToBeginningOfNextWord(): void;
        /**
         * Expand selections to the beginning of the next paragraph.
         * 
         * Operates on all selections. Moves the cursor to the beginning of the next
         * paragraph while preserving the selection's tail position. 
         */
        selectToBeginningOfNextParagraph(): void;
        /**
         * Expand selections to the beginning of the next paragraph.
         * 
         * Operates on all selections. Moves the cursor to the beginning of the next
         * paragraph while preserving the selection's tail position. 
         */
        selectToBeginningOfPreviousParagraph(): void;
        /**
         * Select the range of the given marker if it is valid.
         * @param {Marker} A {Marker}
         * @returns {Range} Returns the selected {Range} or `` if the marker is invalid.
         */
        selectMarker(marker: Marker): Range;
        /**
         * Get the most recently added {Selection}.
         * @returns {Selection} Returns a {Selection}.
         */
        getLastSelection(): Selection;
        /**
         * Get current {Selection}s.
         * @returns {any[]} Returns: An {Array} of {Selection}s.
         */
        getSelections(): any[];
        /**
         * Get all {Selection}s, ordered by their position in the buffer
         * instead of the order in which they were added.
         * @returns {any[]} Returns an {Array} of {Selection}s.
         */
        getSelectionsOrderedByBufferPosition(): any[];
        /**
         * Determine if a given range in buffer coordinates intersects a
         * selection.
         * @param {Range} A {Range} or range-compatible {Array}.
         * @returns {boolean} Returns a {Boolean}.
         */
        selectionIntersectsBufferRange(bufferRange: Range): boolean;
        /**
         * Scan regular expression matches in the entire buffer, calling the
         * given iterator function on each match.
         * 
         * `::scan` functions as the replace method as well via the `replace`
         * 
         * If you're programmatically modifying the results, you may want to try
         * {::backwardsScanInBufferRange} to avoid tripping over your own changes.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Function} A {Function} that's called on each match
         */
        scan(regex: RegExp, iterator: Function): void;
        /**
         * Scan regular expression matches in a given range, calling the given
         * iterator function on each match.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Range} A {Range} in which to search.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        scanInBufferRange(regex: RegExp, range: Range, iterator: Function): void;
        /**
         * Scan regular expression matches in a given range in reverse order,
         * calling the given iterator function on each match.
         * @param {RegExp} A {RegExp} to search for.
         * @param {Range} A {Range} in which to search.
         * @param {Function} A {Function} that's called on each match with an {Object} containing the following keys:
         */
        backwardsScanInBufferRange(regex: RegExp, range: Range, iterator: Function): void;
        /**
         * @returns {boolean} Returns a {Boolean} indicating whether softTabs are enabled for this
        editor.
         */
        getSoftTabs(): boolean;
        /**
         * Enable or disable soft tabs for this editor.
         * @param {boolean} A {Boolean} 
         */
        setSoftTabs(softTabs: boolean): void;
        /**
         * Toggle soft tabs for this editor 
         */
        toggleSoftTabs(): void;
        /**
         * Get the on-screen length of tab characters.
         * @returns {number} Returns a {Number}.
         */
        getTabLength(): number;
        /**
         * Set the on-screen length of tab characters. Setting this to a
         * {Number} This will override the `editor.tabLength` setting.
         * @param {number} {Number} length of a single tab. Setting to `null` will fallback to using the `editor.tabLength` config setting 
         */
        setTabLength(tabLength: number): void;
        /**
         * Determine if the buffer uses hard or soft tabs.
         * @returns  Returns `true` if the first non-comment line with leading whitespace starts
        with a space character.
         * @returns  Returns `false` if it starts with a hard tab (`\t`).
         * @returns {boolean} Returns a {Boolean} or  if no non-comment lines had leading
        whitespace.
         */
        usesSoftTabs(): boolean;
        /**
         * Get the text representing a single level of indent.
         * 
         * If soft tabs are enabled, the text is composed of N spaces, where N is the
         * tab length. Otherwise the text is a tab character (`\t`).
         * @returns {string} Returns a {String}.
         */
        getTabText(): string;
        /**
         * Determine whether lines in this editor are soft-wrapped.
         * @returns {boolean} Returns a {Boolean}.
         */
        isSoftWrapped(): boolean;
        /**
         * Enable or disable soft wrapping for this editor.
         * @param {boolean} A {Boolean}
         * @returns {boolean} Returns a {Boolean}.
         */
        setSoftWrapped(softWrapped: boolean): boolean;
        /**
         * Toggle soft wrapping for this editor
         * @returns {boolean} Returns a {Boolean}.
         */
        toggleSoftWrapped(): boolean;
        /**
         * Gets the column at which column will soft wrap 
         */
        getSoftWrapColumn(): void;
        /**
         * Get the indentation level of the given a buffer row.
         * @returns {number} Returns how deeply the given row is indented based on the soft tabs and
        tab length settings of this editor. Note that if soft tabs are enabled and
        the tab length is 2, a row with 4 leading spaces would have an indentation
        level of 2.
        
        * `bufferRow` A {Number} indicating the buffer row.
         * @returns {number} Returns a {Number}.
         */
        indentationForBufferRow(): number;
        /**
         * Set the indentation level for the given buffer row.
         * 
         * Inserts or removes hard tabs or spaces based on the soft tabs and tab length
         * settings of this editor in order to bring it to the given indentation level.
         * Note that if soft tabs are enabled and the tab length is 2, a row with 4
         * leading spaces would have an indentation level of 2.
         * @param {number} A {Number} indicating the buffer row.
         * @param {number} A {Number} indicating the new indentation level.
         * @param {Object} An {Object} with the following keys:
         */
        setIndentationForBufferRow(bufferRow: number, newLevel: number, options?: Object): void;
        /**
         * Indent rows intersecting selections by one level. 
         */
        indentSelectedRows(): void;
        /**
         * Outdent rows intersecting selections by one level. 
         */
        outdentSelectedRows(): void;
        /**
         * Get the indentation level of the given line of text.
         * @returns {string} Returns how deeply the given line is indented based on the soft tabs and
        tab length settings of this editor. Note that if soft tabs are enabled and
        the tab length is 2, a row with 4 leading spaces would have an indentation
        level of 2.
        
        * `line` A {String} representing a line of text.
         * @returns {number} Returns a {Number}.
         */
        indentLevelForLine(): string | number;
        /**
         * Indent rows intersecting selections based on the grammar's suggested
         * indent level. 
         */
        autoIndentSelectedRows(): void;
        /**
         * Get the current {Grammar} of this editor. 
         */
        getGrammar(): void;
        /**
         * Set the current {Grammar} of this editor.
         * 
         * Assigning a grammar will cause the editor to re-tokenize based on the new
         * grammar.
         * @param {Grammar} {Grammar} 
         */
        setGrammar(grammar: Grammar): void;
        /**
         * @returns {ScopeDescriptor} Returns a {ScopeDescriptor} that includes this editor's language.
        e.g. `['.source.ruby']`, or `['.source.coffee']`. You can use this with
        {Config::get} to get language specific config values.
         */
        getRootScopeDescriptor(): ScopeDescriptor;
        /**
         * Get the syntactic scopeDescriptor for the given position in buffer
         * coordinates. Useful with {Config::get}.
         * 
         * For example, if called with a position inside the parameter list of an
         * anonymous CoffeeScript function, the method returns the following array:
         * `["source.coffee", "meta.inline.function.coffee", "variable.parameter.function.coffee"]`
         * @param {Point} A {Point} or {Array} of [row, column].
         * @returns {ScopeDescriptor} Returns a {ScopeDescriptor}.
         */
        scopeDescriptorForBufferPosition(bufferPosition: Point): ScopeDescriptor;
        /**
         * Get the range in buffer coordinates of all tokens surrounding the
         * cursor that match the given scope selector.
         * 
         * For example, if you wanted to find the string surrounding the cursor, you
         * could call `editor.bufferRangeForScopeAtCursor(".string.quoted")`.
         * @param {string} {String} selector. e.g. `'.source.ruby'`
         * @returns {Range} Returns a {Range}.
         */
        bufferRangeForScopeAtCursor(scopeSelector: string): Range;
        /**
         * Determine if the given row is entirely a comment 
         */
        isBufferRowCommented(): void;
        /**
         * For each selection, copy the selected text. 
         */
        copySelectedText(): void;
        /**
         * For each selection, cut the selected text. 
         */
        cutSelectedText(): void;
        /**
         * For each selection, replace the selected text with the contents of
         * the clipboard.
         * 
         * If the clipboard contains the same number of selections as the current
         * editor, each selection will be replaced with the content of the
         * corresponding clipboard selection text.
         * @param  See {Selection::insertText}. 
         */
        pasteText(options?: any): void;
        /**
         * For each selection, if the selection is empty, cut all characters
         * of the containing line following the cursor. Otherwise cut the selected
         * text. 
         */
        cutToEndOfLine(): void;
        /**
         * Fold the most recent cursor's row based on its indentation level.
         * 
         * The fold will extend from the nearest preceding line with a lower
         * indentation level up to the nearest following row with a lower indentation
         * level. 
         */
        foldCurrentRow(): void;
        /**
         * Unfold the most recent cursor's row by one level. 
         */
        unfoldCurrentRow(): void;
        /**
         * Fold the given row in buffer coordinates based on its indentation
         * level.
         * 
         * If the given row is foldable, the fold will begin there. Otherwise, it will
         * begin at the first foldable row preceding the given row.
         * @param {number} A {Number}. 
         */
        foldBufferRow(bufferRow: number): void;
        /**
         * Unfold all folds containing the given row in buffer coordinates.
         * @param {number} A {Number} 
         */
        unfoldBufferRow(bufferRow: number): void;
        /**
         * For each selection, fold the rows it intersects. 
         */
        foldSelectedLines(): void;
        /**
         * Fold all foldable lines. 
         */
        foldAll(): void;
        /**
         * Unfold all existing folds. 
         */
        unfoldAll(): void;
        /**
         * Fold all foldable lines at the given indent level.
         * @param {number} A {Number}. 
         */
        foldAllAtIndentLevel(level: number): void;
        /**
         * Determine whether the given row in buffer coordinates is foldable.
         * 
         * A *foldable* row is a row that *starts* a row range that can be folded.
         * @param {number} A {Number}
         * @returns {boolean} Returns a {Boolean}.
         */
        isFoldableAtBufferRow(bufferRow: number): boolean;
        /**
         * Determine whether the given row in screen coordinates is foldable.
         * 
         * A *foldable* row is a row that *starts* a row range that can be folded.
         * @param {number} A {Number}
         * @returns {boolean} Returns a {Boolean}.
         */
        isFoldableAtScreenRow(bufferRow: number): boolean;
        /**
         * Fold the given buffer row if it isn't currently folded, and unfold
         * it otherwise. 
         */
        toggleFoldAtBufferRow(): void;
        /**
         * Determine whether the most recently added cursor's row is folded.
         * @returns {boolean} Returns a {Boolean}.
         */
        isFoldedAtCursorRow(): boolean;
        /**
         * Determine whether the given row in buffer coordinates is folded.
         * @param {number} A {Number}
         * @returns {boolean} Returns a {Boolean}.
         */
        isFoldedAtBufferRow(bufferRow: number): boolean;
        /**
         * Determine whether the given row in screen coordinates is folded.
         * @param {number} A {Number}
         * @returns {boolean} Returns a {Boolean}.
         */
        isFoldedAtScreenRow(screenRow: number): boolean;
        /**
         * Scroll the editor to reveal the most recently added cursor if it is
         * off-screen.
         * @param {Object} {Object}
         */
        scrollToCursorPosition(options?: Object): void;
        /**
         * Scrolls the editor to the given buffer position.
         * @param {Object} An object that represents a buffer position. It can be either an {Object} (`{row, column}`), {Array} (`[row, column]`), or {Point}
         * @param {Object} {Object}
         */
        scrollToBufferPosition(bufferPosition: Object, options?: Object): void;
        /**
         * Scrolls the editor to the given screen position.
         * @param {Object} An object that represents a buffer position. It can be either  an {Object} (`{row, column}`), {Array} (`[row, column]`), or {Point}
         * @param {Object} {Object}
         */
        scrollToScreenPosition(screenPosition: Object, options?: Object): void;
        /**
         * Scrolls the editor to the top 
         */
        scrollToTop(): void;
        /**
         * Scrolls the editor to the bottom 
         */
        scrollToBottom(): void;
        /**
         * Retrieves the greyed out placeholder of a mini editor.
         * @returns {string} Returns a {String}.
         */
        getPlaceholderText(): string;
        /**
         * Set the greyed out placeholder of a mini editor. Placeholder text
         * will be displayed when the editor has no content.
         * @param {string} {String} text that is displayed when the editor has no content. 
         */
        setPlaceholderText(placeholderText: string): void;
    }

    /**
     * Handles loading and activating available themes.
     * 
     * An instance of this class is always available as the `atom.themes` global. 
     *
     * file: src/theme-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/theme-manager.coffee#L13
     */
    class ThemeManager { 
        /**
         * Invoke `callback` when style sheet changes associated with
         * updating the list of active themes have completed.
         * @param {Function} {Function} 
         */
        onDidChangeActiveThemes(callback: Function): void;
        /**
         * Get an array of all the loaded theme names. 
         */
        getLoadedThemeNames(): void;
        /**
         * Get an array of all the loaded themes. 
         */
        getLoadedThemes(): void;
        /**
         * Get an array of all the active theme names. 
         */
        getActiveThemeNames(): void;
        /**
         * Get an array of all the active themes. 
         */
        getActiveThemes(): void;
        /**
         * Get the enabled theme names from the config.
         * @returns  Returns an array of theme names in the order that they should be activated.
         */
        getEnabledThemeNames(): any;
    }

    /**
     * Associates tooltips with HTML elements or selectors.
     * 
     * You can get the `TooltipManager` via `atom.tooltips`.
     *
     * file: src/tooltip-manager.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/tooltip-manager.coffee#L47
     */
    class TooltipManager { 
        /**
         * Add a tooltip to the given element.
         * @param  An `HTMLElement`
         * @param  See http://getbootstrap.com/javascript/#tooltips for a full list of options. You can also supply the following additional options:
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        tooltip.
         */
        add(target: any, options: any): Disposable;
    }

    /**
     * `ViewRegistry` handles the association between model and view
     * types in Atom. We call this association a View Provider. As in, for a given
     * model, this class can provide a view via {::getView}, as long as the
     * model/view association was registered via {::addViewProvider}
     * 
     * If you're adding your own kind of pane item, a good strategy for all but the
     * simplest items is to separate the model and the view. The model handles
     * application logic and is the primary point of API interaction. The view
     * just handles presentation.
     * 
     * View providers inform the workspace how your model objects should be
     * presented in the DOM. A view provider must always return a DOM node, which
     * makes [HTML 5 custom elements](http://www.html5rocks.com/en/tutorials/webcomponents/customelements/)
     * an ideal tool for implementing views in Atom.
     * 
     * You can access the `ViewRegistry` object via `atom.views`.
     *
     * file: src/view-registry.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/view-registry.coffee#L44
     */
    class ViewRegistry { 
        /**
         * Add a provider that will be used to construct views in the
         * workspace's view layer based on model objects in its model layer.
         * @param {Object} {Object} containing the following keys:
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        added provider.
         */
        addViewProvider(providerSpec: Object): Disposable;
        /**
         * Get the view associated with an object in the workspace.
         * 
         * If you're just *using* the workspace, you shouldn't need to access the view
         * layer, but view layer access may be necessary if you want to perform DOM
         * manipulation that isn't supported via the model API.
         * @param  The object for which you want to retrieve a view. This can be a pane item, a pane, or the workspace itself.
         * @returns  Returns a DOM element.
         */
        getView(object: any): any;
    }

    /**
     * Represents the state of the user interface for the entire window.
     * An instance of this class is available via the `atom.workspace` global.
     * 
     * Interact with this object to open files, be notified of current and future
     * editors, and manipulate panes. To add panels, you'll need to use the
     * {WorkspaceView} class for now until we establish APIs at the model layer.
     *
     * file: src/workspace.coffee
     * srcUrl: https://github.com/atom/atom/blob/v0.193.0/src/workspace.coffee#L31
     */
    class Workspace { 
        /**
         * Invoke the given callback with all current and future text
         * editors in the workspace.
         * @param {Function} {Function} to be called with current and future text editors.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeTextEditors(callback: Function): Disposable;
        /**
         * Invoke the given callback with all current and future panes items
         * in the workspace.
         * @param {Function} {Function} to be called with current and future pane items.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observePaneItems(callback: Function): Disposable;
        /**
         * Invoke the given callback when the active pane item changes.
         * @param {Function} {Function} to be called when the active pane item changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeActivePaneItem(callback: Function): Disposable;
        /**
         * Invoke the given callback with the current active pane item and
         * with all future active pane items in the workspace.
         * @param {Function} {Function} to be called when the active pane item changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeActivePaneItem(callback: Function): Disposable;
        /**
         * Invoke the given callback whenever an item is opened. Unlike
         * {::onDidAddPaneItem}, observers will be notified for items that are already
         * present in the workspace when they are reopened.
         * @param {Function} {Function} to be called whenever an item is opened.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidOpen(callback: Function): Disposable;
        /**
         * Invoke the given callback when a pane is added to the workspace.
         * @param {Function} {Function} to be called panes are added.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddPane(callback: Function): Disposable;
        /**
         * Invoke the given callback when a pane is destroyed in the
         * workspace.
         * @param {Function} {Function} to be called panes are destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidDestroyPane(callback: Function): Disposable;
        /**
         * Invoke the given callback with all current and future panes in the
         * workspace.
         * @param {Function} {Function} to be called with current and future panes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observePanes(callback: Function): Disposable;
        /**
         * Invoke the given callback when the active pane changes.
         * @param {Function} {Function} to be called when the active pane changes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidChangeActivePane(callback: Function): Disposable;
        /**
         * Invoke the given callback with the current active pane and when
         * the active pane changes.
         * @param {Function} {Function} to be called with the current and future active# panes.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        observeActivePane(callback: Function): Disposable;
        /**
         * Invoke the given callback when a pane item is added to the
         * workspace.
         * @param {Function} {Function} to be called when pane items are added.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddPaneItem(callback: Function): Disposable;
        /**
         * Invoke the given callback when a pane item is about to be
         * destroyed, before the user is prompted to save it.
         * @param {Function} {Function} to be called before pane items are destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose` can be called to unsubscribe.
         */
        onWillDestroyPaneItem(callback: Function): Disposable;
        /**
         * Invoke the given callback when a pane item is destroyed.
         * @param {Function} {Function} to be called when pane items are destroyed.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose` can be called to unsubscribe.
         */
        onDidDestroyPaneItem(callback: Function): Disposable;
        /**
         * Invoke the given callback when a text editor is added to the
         * workspace.
         * @param {Function} {Function} to be called panes are added.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to unsubscribe.
         */
        onDidAddTextEditor(callback: Function): Disposable;
        /**
         * Opens the given URI in Atom asynchronously.
         * If the URI is already open, the existing item for that URI will be
         * activated. If no URI is given, or no registered opener can open
         * the URI, a new empty {TextEditor} will be created.
         * @param {string} A {String} containing a URI.
         * @param {Object} {Object}
         * @returns {TextEditor} Returns a promise that resolves to the {TextEditor} for the file URI.
         */
        open(uri?: string, options?: Object): TextEditor;
        /**
         * Asynchronously reopens the last-closed item's URI if it hasn't already been
         * reopened.
         * @returns  Returns a promise that is resolved when the item is opened
         */
        reopenItem(): any;
        /**
         * Register an opener for a uri.
         * 
         * An {TextEditor} will be used if no openers return a value.
         * @param {Function} A {Function} to be called when a path is being opened.
         * @returns {Disposable} Returns a {Disposable} on which `.dispose()` can be called to remove the
        opener.
         */
        addOpener(opener: Function): Disposable;
        /**
         * Get all pane items in the workspace.
         * @returns {any[]} Returns an {Array} of items.
         */
        getPaneItems(): any[];
        /**
         * Get the active {Pane}'s active item.
         * @returns {Object} Returns an pane item {Object}.
         */
        getActivePaneItem(): Object;
        /**
         * Get all text editors in the workspace.
         * @returns {any[]} Returns an {Array} of {TextEditor}s.
         */
        getTextEditors(): any[];
        /**
         * Get the active item if it is an {TextEditor}.
         * @returns {TextEditor} Returns an {TextEditor} or `` if the current active item is not an
        {TextEditor}.
         */
        getActiveTextEditor(): TextEditor;
        /**
         * Get all panes in the workspace.
         * @returns {any[]} Returns an {Array} of {Pane}s.
         */
        getPanes(): any[];
        /**
         * Get the active {Pane}.
         * @returns {Pane} Returns a {Pane}.
         */
        getActivePane(): Pane;
        /**
         * Make the next pane active. 
         */
        activateNextPane(): void;
        /**
         * Make the previous pane active. 
         */
        activatePreviousPane(): void;
        /**
         * Get the first {Pane} with an item for the given URI.
         * @param {string} {String} uri
         * @returns {Pane} Returns a {Pane} or `` if no pane exists for the given URI.
         */
        paneForURI(uri: string): Pane;
        /**
         * Get the {Pane} containing the given item.
         * @param  Item the returned pane contains.
         * @returns {Pane} Returns a {Pane} or `` if no pane exists for the given item.
         */
        paneForItem(item: any): Pane;
        /**
         * Get an {Array} of all the panel items at the bottom of the editor window. 
         */
        getBottomPanels(): void;
        /**
         * Adds a panel item to the bottom of the editor window.
         * @param {Object} {Object}
         * @returns {Panel} Returns a {Panel}
         */
        addBottomPanel(options: Object): Panel;
        /**
         * Get an {Array} of all the panel items to the left of the editor window. 
         */
        getLeftPanels(): void;
        /**
         * Adds a panel item to the left of the editor window.
         * @param {Object} {Object}
         * @returns {Panel} Returns a {Panel}
         */
        addLeftPanel(options: Object): Panel;
        /**
         * Get an {Array} of all the panel items to the right of the editor window. 
         */
        getRightPanels(): void;
        /**
         * Adds a panel item to the right of the editor window.
         * @param {Object} {Object}
         * @returns {Panel} Returns a {Panel}
         */
        addRightPanel(options: Object): Panel;
        /**
         * Get an {Array} of all the panel items at the top of the editor window. 
         */
        getTopPanels(): void;
        /**
         * Adds a panel item to the top of the editor window above the tabs.
         * @param {Object} {Object}
         * @returns {Panel} Returns a {Panel}
         */
        addTopPanel(options: Object): Panel;
        /**
         * Get an {Array} of all the modal panel items 
         */
        getModalPanels(): void;
        /**
         * Adds a panel item as a modal dialog.
         * @param {Object} {Object}
         * @returns {Panel} Returns a {Panel}
         */
        addModalPanel(options: Object): Panel;
        /**
         * @param  Item the panel contains 
         * @returns {Panel} Returns the {Panel} associated with the given item.
         * @returns  Returns
        `null` when the item has no panel.
         */
        panelForItem(item: any): Panel;
        /**
         * Performs a search across all the files in the workspace.
         * @param {RegExp} {RegExp} to search with.
         * @param {Object} {Object} (default: {})
         * @param {Function} {Function} callback on each file found
         * @returns  Returns a `Promise`.
         */
        scan(regex: RegExp, options?: Object, iterator?: Function): any;
        /**
         * Performs a replace across all the specified files in the project.
         * @param {RegExp} A {RegExp} to search with.
         * @param  Text to replace all matches of regex with
         * @param  List of file path strings to run the replace on.
         * @param {Function} A {Function} callback on each file with replacements:
         * @returns  Returns a `Promise`.
         */
        replace(regex: RegExp, replacementText: any, filePaths: any, iterator: Function): any;
    }

}
