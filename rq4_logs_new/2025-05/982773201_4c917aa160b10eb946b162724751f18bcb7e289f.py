

try:
    # Open the input file in read mode ("r")
    input_file = open("teset.txt", "r")
    content = input_file.read()  # Read the full content of the file
    input_file.close()  # Close the input file after reading

    # Modify the content i.e convert it to uppercase)
    modified_content = content.upper()

    

    # Open the output file in write mode ("w")
    output_file = open("teset2.txt", "w")
    output_file.write(modified_content)  # Write the modified content
    output_file.close()  # Close the output file after writing

    print("Modified content has been written to", "teset2.txt")

except FileNotFoundError:
    print("Error: The input file was not found. Make sure it exists.")
except IOError:
    print(" Error: Something went wrong reading or writing the files.")