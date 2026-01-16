// Imports
import Book_Handler from "./book_handler.js"

class UI_Handler{
    constructor(){
        this.book_handler = new Book_Handler();
        this.status_colours = {
            GREEN:"#419985",
            RED:"#E53E31"
        }
        this._initialize();
    }

    _initialize(){
        // Binding pointer to in-class functions => bind to event listener (button click), instead of btn_onclick (global_scope)
        // - Alternative option: rebind ui_handler to be globally scoped with `window.ui_handler = ui_handler()` at class_instantiation
//        this.get_random_book_data = this.get_random_book_data.bind(this); // Bind method so `this` points to existing UI_Handler instance
//        document.getElementById("btn_get_random_book_call").addEventListener("click", this.get_random_book_data); // this.function.bind(null, *args) if any args
//
//        this.get_search_result = this.get_search_result.bind(this)
//        document.getElementById("btn_book_search").addEventListener("click", this.get_search_result)
    }

    async get_random_book_data(){
        try{
            const book = await this.book_handler.retrieve_random_book_data();
            this.update_book_UI(book);
        } catch (error) {
            console.error(error.message)
        }
    }

    async get_book_data(work_id){
        try{
            const book = await this.book_handler.retrieve_book_data(work_id);
            this.update_book_UI(book);
        } catch (error) {
            console.error(error.message)
        }
    }

    async get_search_result(){
        // Note: Do not even save the raw_search_query before sanitizing it
        let sanitized_query = this.book_handler.sanitize_query(document.getElementById("search_request").value);
        console.log("Sanitized Query: ")

        // Get Raw Query => Sanitize Query => Validate S_Query => API_Request(S_Query)
        if (sanitized_query == null || sanitized_query == ""){
            console.log("Custom Error: Search Prompt is empty!")
            return;
        }
        console.log(sanitized_query)

        try{
            const search_result = await this.book_handler.retrieve_search_result(sanitized_query)
            this.update_search_UI(search_result);
        } catch (error){
            console.error(error.message)
        }
    }

    // Security Note: Do not RETRIEVE book data from .innerHTML due to inspect element, only SET
    update_book_UI(book){
        console.log(book)
        const display_prompt = document.getElementById("UI");
        const typing_prompt_sentence = this.book_handler.get_book_field_data(book, "first_sentence").value;
        display_prompt.innerHTML = `${typing_prompt_sentence}<span class="blinking-cursor">|</span>`;

        this.development_console_logging(book)
//        this.display_book_metadata(book)
//        this.display_book_cover(book)
    }

    update_search_UI(search_result){
        console.log("[[Search Display Results]]")
        this.development_console_logging(search_result)
        this.display_search_result(search_result)
        this.display_book_cover(search_result)
    }

    display_book_metadata(book){
        const _field_names = this.book_handler.get_book_field_names(book);
        const _missing_field_names = this.book_handler.get_book_missing_field_names(book);
        let output_string = ""
        if (this.book_handler.is_missing_field_data(book)){

            let error_fields = _missing_field_names.join(", ");
//            document.getElementById("status_component_name").innerHTML = "Status - Missing the following fields: " + error_fields;
//            document.getElementById("status_component_name").style.color = this.status_colours.RED;
        }
        else{
//            document.getElementById("status_component_name").innerHTML = "Status: OK";
//            document.getElementById("status_component_name").style.color = this.status_colours.GREEN;
        }

        for (let i = 0; i < _field_names.length; i++){
            const _current_field_name = _field_names[i];
            if (_missing_field_names.includes(_current_field_name)){
                output_string += (_current_field_name + ": " + "null (missing)" + "<br />");
            }
            else{
                output_string += (_current_field_name + ": " + this.book_handler.get_book_field_data(book, _current_field_name) + "<br />");
            }
        }
//        document.getElementById("component_name").innerHTML = output_string;
    }

    display_book_cover(book){
        var image = document.createElement('IMG');
        this.book_handler.create_book_cover_URL(book)
        image.src = this.book_handler.get_book_cover_URL(book);
//        document.getElementById("component_name").appendChild(image)
    }

    development_console_logging(book){
        console.log("Parsed Book Data:")
        console.log(book)
        console.log(this.book_handler.get_book_field_names(book));
        console.log(this.book_handler.get_book_metadata(book));
        this.book_handler.print_book_missing_fields(book);
        console.log("Book Data (Title): " + this.book_handler.get_book_field_data(book, "title"));
    }

    display_search_result(search_result){
        // Display Search Result components
    }
} // End Class

const ui_handler = new UI_Handler();
export default ui_handler;