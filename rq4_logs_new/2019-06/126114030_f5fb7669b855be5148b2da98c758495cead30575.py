import docx
import os

''' Function: read_file(file_name, item_type)
Parameters: file_name - the name of the file to be read.
            item_type - user-inputted search term to specify what is parsed for in the document.
            os.path.dirname(os.path.abspath(__file__))+"\\" is the current working directory.

Returns:    parsed_data_list - A list of parsed data (strings) from the parsed text documents.
                               This list will be used to append the item_type_master_list to be written to a .txt file.

Desc: Function intended to read files and return a list (parsed_data_list) of text lines starting at the search
keyword and ending at the end of the line.

The function creates an object (doc) which is defined by the contents of the document's paragraphs,
then creates an empty list (parsed_data_list). For loop looks through each line inside (doc) for search word (item_type)
specified by the user.

If item_type found, prints a string from the start of the line item_type was found on to the end of the length of the 
line that item_type exists on and appends the parsed_data_list with it.
'''


def read_file(file_name, item_type):

    try:
        # Convert file contents to string.
        document_paragraphs = docx.Document(file_name).paragraphs

        # Replace document file extensions with empty strings and add the new strings to parsed_data_list.
        parsed_data_list = [file_name.replace(".docx", "")]

        # Check user-entered item is in the documents and append the parsed_data_list with the line from the document.
        for text_lines in document_paragraphs:

            if item_type in text_lines.text:
                parsed_data_list.append(text_lines.text[:text_lines.text.find(item_type)] + text_lines.text[text_lines.text.find(item_type):])

            elif item_type.upper() in text_lines.text:
                parsed_data_list.append(text_lines.text[:text_lines.text.find(item_type.upper())] + text_lines.text[text_lines.text.find(item_type.upper()):])

            elif item_type.lower() in text_lines.text:
                parsed_data_list.append(text_lines.text[:text_lines.text.find(item_type.lower())] + text_lines.text[text_lines.text.find(item_type.lower()):])

        return parsed_data_list

    except Exception:
        print("[Error:", file_name, "is not a .document_paragraphs or .docx file.]")


'''Function: parse_directory(directory)
Parameter: directory - The current or specified directory which documents are searched from.

Returns: files_in_dir_list - A list of file names that were found within the directory.

Desc: Creates list (files_in_dir_list) of .docx or .doc files the current directory.'''


def parse_directory(directory):

    files_in_dir_list = []

    for (dirpath, dirnames, filenames) in os.walk(directory):
        print("\n\nFiles found in directory ", dirpath, ": \n", filenames, "\n")

        for files in filenames:
            if (".docx" in files) or (".doc" in files):
                files_in_dir_list.append(files)

            else:
                pass

        break

    return files_in_dir_list


'''Function: collect_motions(directory, item_type, item_list)
Parameters: directory - The current or specified directory which documents are searched from.
            item_type - The user-entered search term.
            item_list - list of lines in directory Word files that correspond to the searched item_type.
            
Returns: item_list - list of found data alongside its instance of item_type.

Desc: Used to append a master list (item_list), passed in the program as item_type_master_list, of lines where 
item_type was found.'''


def collect_motions(directory, item_type, item_list):

    try:
        filenames = parse_directory(directory)

        for file in filenames:

            file_exists = read_file(file, item_type)

            if file_exists:
                # Make lines in item_type_master_list to separate motions.
                item_list.append("\n\n" + "-" * 100)
                item_list.extend(file_exists)

        return item_list

    except TypeError:
        print("[Error: Cannot build list of filenames.]")


'''Function: write_text_file(data, user_item)
Parameters: data - The list containing the data to be written to a text file.
            user_item - User-entered search term. Used in the output file to specify what was searched for.
            counter - Counter to keep track of number of files outputted so far this run. Used to create output_x.txt
                      file names where x = counter.

Returns: None. Writes data to file, closes file.

Desc: Call to enter found data parameter (in this case a list) into a new text file, write and close the file.'''


def write_text_file(data, user_item, counter):

    if type(user_item) == str:

        # Concatenate the name of the file to be created using the user-specified item that was found in the Word Docs.
        new_file_name = "output_" + str(counter) + ".txt"

        # Turn data (master list of found strings) into a string that can be written to a new file.
        text_file = open(new_file_name, 'w')
        data_to_write = "Search for '" + user_item + "'. Files are labelled and separated by lines.\n\n" + '\n'.join(data)

        text_file.write(data_to_write)
        text_file.close()

    else:
        print("Error: Output file not created. Invalid user search keyword or bug.")
        pass


def main():

    # Define current dir variable to be referred to throughout program. Create empty list for final parsed data.
    current_directory = os.path.dirname(os.path.abspath(__file__))
    item_type_master_list = []
    output_count = 0

    print("RatiFinder v. 0.2. (Terminal) -- Christian Pearson and Darren Berg 2018")
    print("Automate compiling of motions, actions or other search-terms from Word files such as Meeting Minutes.")
    print("\nStep 1: Copy Word documents to search into folder that contains your RatiFinder.exe file.")
    print("Step 2: Run program and follow the prompts on screen.")
    print("Step 3: Check folder for output.txt file containing list of document lines found with search keyword.")
    print("-" * 100, "\n")

    loop_check = 1

    # Begin main program menu loop. User selects one of three options using 1, 2 or 3 as terms.
    while loop_check != -1:
        print("1) Directory Search    -- search entire folder the .exe is in. [Recommended]")
        print("2) Single File Search  -- specify one file to search.")
        print("3) End program")

        # User chooses one of the three options by input.
        user_choice = input("Please enter 1, 2 or 3, corresponding to above options: ").casefold()

        # Search whole directory.
        if user_choice == "1":
            print("-" * 100 + "\n")
            print("The current directory is ", current_directory + "\\")

            # User enters search term to search current dir documents for.
            item_type = input("Please enter EXACT keyword you are searching for (eg. motion, action): ").casefold()

            try:
                for files in collect_motions(current_directory + "\\", item_type, item_type_master_list):
                    print(files)

            except TypeError:
                print("[Error finding ", item_type, " No files found in directory.]")

            try:
                # Write data to .txt file.
                write_text_file(item_type_master_list, item_type, output_count)
                output_count += 1

                # Clear content of list for next search.
                item_type_master_list = []

            except Exception:
                print("Error: Unable to write data to text file.")

            # User decides if to break the loop.
            print("\nSearch for '", item_type, "' complete.")

            end_choice = input("\nKeyword instances collected. If nothing visible then no files/keywords were found. "
                               "Would you like to rerun the program? (y/n) ")

            if (end_choice == "y") or (end_choice == "yes") or (end_choice == "Yes"):
                loop_check = 1

            else:
                loop_check = -1

        # Search a single file.
        elif user_choice == "2":
            print("\n" + "-" * 100)
            print("The current directory is ", current_directory + "\\" + "\n")
            print("\n" + "-" * 100)

            # User defines the item to search for.
            item_type = input("Please enter the type of item you are searching for (motion, action): ").casefold()

            # Print files in directory to help user choose.
            try:
                print(".doc and .docx files found in directory: \n\n", parse_directory(current_directory))

            except TypeError:
                print("[Error finding ", item_type, " No files found in directory.]\n")

            # User enters file to search.
            path = input("\nPlease enter the filename: ")

            # Correct potential omission of (.docx) file type.
            if ".docx" not in path:
                path += ".docx"

            try:
                for files in read_file(path, item_type):
                    print(files)

            except TypeError:
                print("[Read File Error: Cannot construct list of ", item_type, " in non-existent file.]\n", "-" * 100)

            try:
                # Write data to .txt file.
                write_text_file(read_file(path, item_type), item_type, output_count)

            except Exception:
                print("Error: Unable to write data to text file.")

            loop_check = -1

        # User wants to end the program.
        elif user_choice == "3":
            break

        # Catch invalid user selections.
        else:
            print("[Input Error: Input must be 1, 2 or 3.]\n", "-" * 100)

    print("\nProgram complete. Thanks for using RatiFinder v. 0.1! Graphical functionality will be in future versions. "
          "For more information please see README.txt. To report bugs please email christian.pearson@stemist.ca")

    input("\nPress enter to end program. ")


if __name__ == '__main__':
    main()