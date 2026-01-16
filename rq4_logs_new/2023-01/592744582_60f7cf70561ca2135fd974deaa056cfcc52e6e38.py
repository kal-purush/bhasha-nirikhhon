import csv

def main(file_name):
    # Create a dictionary to store the counts for each source
    source_counts = {}

    # Create a dictionary to store the sources for each email and phone combination
    user_sources = {}

    # Open the file and read it line by line
    with open(file_name) as file:
        # Create a csv reader
        reader = csv.reader(file)
        
        # Skip the header
        next(reader)

        # Iterate over the rows in the file
        for row in reader:
            # Skip empty or incomplete rows
            if any(field == '' for field in row):
                continue

            # Get the email, phone, and source for this entry
            email, phone, source = row

            # Get the set of sources for this user
            sources = user_sources.get(email + phone)
            if sources is None:
                sources = set()
                user_sources[email + phone] = sources

            # Check if this user has been counted for this source before
            if source not in sources:
                # This is a new source for this user, so increment the count for this source
                source_counts[source] = source_counts.get(source, 0) + 1

                # Add this source to the set for this user
                sources.add(source)

    # Print the results
    for source, count in source_counts.items():
        print(f'{source}: {count}')

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Error: No file name provided")
    else:
        main(sys.argv[1])