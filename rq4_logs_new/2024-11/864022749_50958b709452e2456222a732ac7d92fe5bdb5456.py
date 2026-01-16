from prac_07.guitar import Guitar


def main():
    """Main function to load, display, and sort guitars."""
    filename = 'guitars.csv'
    guitars = load_guitars(filename)
    print("All guitars:")
    display_guitars(guitars)
    sort_guitars(guitars)
    print("\nGuitars sorted by year:")
    display_guitars(guitars)
    add_guitars(guitars)
    save_guitars(filename, guitars)


def add_guitars(guitars):
    print("\nAdd new guitars (leave name blank to finish):")
    name = input("Name: ")
    while name:
        try:
            year = int(input("Year: "))
            cost = float(input("Cost: "))
            guitars.append(Guitar(name, year, cost))
        except ValueError:
            print("Invalid input; please enter numbers for year and cost.")
        name = input("Name: ")


def display_guitars(guitars):
    for guitar in guitars:
        vintage_status = " (Vintage)" if guitar.is_vintage() else ""
        print(f"{guitar}{vintage_status}")


def sort_guitars(guitars):
    guitars.sort()


def load_guitars(filename):
    guitars = []
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            name = parts[0]
            year = int(parts[1])
            cost = float(parts[2])
            guitars.append(Guitar(name, year, cost))
    return guitars


def save_guitars(filename, guitars):
    with open(filename, 'w') as file:
        for guitar in guitars:
            file.write(f"{guitar.name},{guitar.year},{guitar.cost}\n")


main()