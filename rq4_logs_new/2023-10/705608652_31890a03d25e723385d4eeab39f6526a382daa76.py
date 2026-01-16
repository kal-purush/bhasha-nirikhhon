import time
import pandas as pd
import numpy as np

# Define the CSV file locations
CITY_DATA = {
    'chicago': 'chicago.csv',
    'new york city': 'new_york_city.csv',
    'washington': 'washington.csv'
}

# Get user filters
def get_filters():
    """
    Asks user to specify a city, month, and day to analyze.

    Returns:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    """
    print("\n")
    print("Welcome to the bike sharing data analysis tool!\n")
    
    # Get user input for city (chicago, new york city, washington)
    while True:
        city = input("Choose a city name (chicago, new york city, washington): ").lower()
        if city in CITY_DATA:
            break
        else:
            print("Invalid city name. Please try again.\n")
    
    # Initialize variables for month and day
    month = 'all'
    day = 'all'
    
    while True:
        # Get user input for filtering by month, day, both, or none
        filter_choice = input("Would you like to filter by month, day, both, or none? ").lower()
        
        if filter_choice == 'none':
            month = 'all'
            day = 'all'
        elif filter_choice == 'month':
            # Get user input for month (all, january, february, ..., june)
            while True:
                month = input("Choose a month to filter by (january, february, march, april, may, or june) or 'all': ").lower()
                if month in ['all', 'january', 'february', 'march', 'april', 'may', 'june']:
                    break
                else:
                    print("Invalid month. Please try again.\n")
            day = 'all'
            break
        elif filter_choice == 'day':
            # Get user input for day of the week (all, monday, tuesday, ..., sunday)
            while True:
                day = input("Choose a day of the week to filter by (e.g., monday, tuesday, ..., sunday) or 'all': ").lower()
                if day in ['all', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
                    break
                else:
                    print("Invalid day of the week. Please try again.\n")
            month = 'all'
            break
        elif filter_choice == 'both':
            # Get user input for month (all, january, february, ..., june)
            while True:
                month = input("Choose a month to filter by (e.g., january, february, ..., june) or 'all': ").lower()
                if month in ['all', 'january', 'february', 'march', 'april', 'may', 'june']:
                    break
                else:
                    print("Invalid month. Please try again.\n")
            
            # Get user input for day of the week (all, monday, tuesday, ..., sunday)
            while True:
                day = input("Choose a day of the week to filter by (e.g., monday, tuesday, ..., sunday) or 'all': ").lower()
                if day in ['all', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
                    break
                else:
                    print("Invalid day of the week. Please try again.\n")
        else:
            continue
            
        # Confirm the chosen filters
        print("\nSelected Filters:")
        print(f"City: {city.title()}")
        print(f"Month: {month.title()}")
        print(f"Day of Week: {day.title()}\n")
        
        confirmation = input("Are these filters correct? Choose 'yes' to proceed or 'no' to edit: ").lower()
        if confirmation == 'yes':
            break  # Proceed with the chosen filters
        elif confirmation == 'no':
            # If the user wants to edit, go back to the filter choice
            continue
        else:
            print("Invalid input. Please Choose 'yes' or 'no'.\n")
            
    return city, month, day

# Lad data based on user filters
def load_data(city, month, day):
    """
    Loads data for the specified city and filters by month and day if applicable.

    Args:
        city (str): Name of the city to analyze
        month (str): Name of the month to filter by, or "all" to apply no month filter
        day (str): Name of the day of week to filter by, or "all" to apply no day filter

    Returns:
        df (pd.DataFrame): Pandas DataFrame containing city data filtered by month and day
    """
    # Load data into a DataFrame
    df = pd.read_csv(CITY_DATA[city])
    
    # Convert the 'Start Time' column to datetime
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    
    # Extract month and day of week from the 'Start Time' column
    df['Month'] = df['Start Time'].dt.month
    df['Day of Week'] = df['Start Time'].dt.strftime('%A').str.lower()
    
    # Filter by month if applicable
    if month != 'all':
        month_num = ['january', 'february', 'march', 'april', 'may', 'june'].index(month) + 1
        df = df[df['Month'] == month_num]
    
    # Filter by day of week if applicable
    if day != 'all':
        df = df[df['Day of Week'] == day]
    
    return df

# Display time statistics
def time_stats(df):
    """
    Displays statistics on the most frequent times of travel.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing city data

    Returns:
        None
    """
    print('-'*80)
    print("\n")

    print("\nTime Statistics...\n")
    start_time = time.time()

    # Mapping month numbers to month names
    month_names = {
        1: 'January',
        2: 'February',
        3: 'March',
        4: 'April',
        5: 'May',
        6: 'June'
    }
    
    # Display the most common month
    common_month_num = df['Month'].mode()[0]
    common_month_name = month_names[common_month_num]
    print(f"Most Common Month: {common_month_name}")
    
    # Display the most common day of week
    common_day = df['Day of Week'].mode()[0].capitalize()
    print(f"Most Common Day of Week: {common_day}")
    
    # Extract the hour from the 'Start Time' column
    df['Hour'] = df['Start Time'].dt.hour
    
    # Display the most common start hour
    common_hour = df['Hour'].mode()[0]
    print(f"Most Common Start Hour: {common_hour}")
    
    print("\n")
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*80)

# Display station statistics
def station_stats(df):
    """
    Displays statistics on the most popular stations and trips.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing city data

    Returns:
        None
    """
    start_time = time.time()
    print("\n")
    
    print("\nStation Statistics...\n")
    
    # Display the most commonly used start station
    common_start_station = df['Start Station'].mode()[0]
    print(f"Most Commonly Used Start Station: {common_start_station}")
    
    # Display the most commonly used end station
    common_end_station = df['End Station'].mode()[0]
    print(f"Most Commonly Used End Station: {common_end_station}")
    
    # Create a new column for the combination of start and end stations
    df['Start-End Station'] = df['Start Station'] + " to " + df['End Station']
    
    # Display the most frequent combination of start and end stations for trips
    common_trip = df['Start-End Station'].mode()[0]
    print(f"Most Frequent Trip (Start Station to End Station): {common_trip}")
    
    print("\n")
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*80)

# Display trip duration statistics
def trip_duration_stats(df):
    """
    Displays statistics on trip duration, including total travel time and mean travel time.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing city data

    Returns:
        None
    """

    start_time = time.time()
    print("\nTrip Duration Statistics...\n")
    
    # Display the total travel time
    total_travel_time = df['Trip Duration'].sum()
    print(f"Total Travel Time: {total_travel_time} seconds")
    
    # Check if 'Gender' column exists in the DataFrame
    if 'Gender' in df.columns:
        # Display total travel time per gender (if available)
        gender_total_travel_time = df.groupby('Gender')['Trip Duration'].sum()
        print("\nTotal Travel Time per Gender:")
        for gender, travel_time in gender_total_travel_time.items():
            print(f"{gender}: {travel_time} seconds")
    else:
        print("\nTotal Travel Time per Gender: No Data Available")
    
    # Display the mean travel time
    mean_travel_time = df['Trip Duration'].mean()
    print(f"\nMean Travel Time: {mean_travel_time} seconds")
    
    print("\n")
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*80)

# Display user statistics
def user_stats(df):
    """
    Displays statistics on bike share users.

    Args:
        df (pd.DataFrame): Pandas DataFrame containing city data

    Returns:
        None
    """

    start_time = time.time()
    print("\nUser Statistics...\n")
    
    # Display counts of user types
    user_type_counts = df['User Type'].value_counts()
    print("User Types Counts:")
    for user_type, count in user_type_counts.items():
        print(f"{user_type}s: {count}")
    
    # Check if 'Gender' column exists in the DataFrame
    if 'Gender' in df.columns:
        # Display counts of gender (if available)
        gender_counts = df['Gender'].value_counts()
        print("\nGender Counts:")
        for gender, count in gender_counts.items():
            print(f"{gender}: {count}")
    else:
        print("\nGender: No Data Available")
    
    # Check if 'Birth Year' column exists in the DataFrame
    if 'Birth Year' in df.columns:
        # Display earliest, most recent, and most common year of birth (if available)
        earliest_birth_year = df['Birth Year'].min()
        most_recent_birth_year = df['Birth Year'].max()
        most_common_birth_year = df['Birth Year'].mode()[0]
        
        print("\nBirth Year Statistics:")
        print(f"Earliest Birth Year: {int(earliest_birth_year)}")
        print(f"Most Recent Birth Year: {int(most_recent_birth_year)}")
        print(f"Most Common Birth Year: {int(most_common_birth_year)}")
    else:
        print("\nBirth Year: No Data Available")
    
    print("\n")
    print("\nThis took %s seconds." % (time.time() - start_time))

def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)

        print("\n")
        print('-'*80)
        print("Analysis Parameters: ")
        print(f"City: {city.title()}")
        print(f"Month: {month.title()}")
        print(f"Day of Week: {day.title()}\n")
        print('-'*80)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)
        
        # Ask the user if they want to see raw data
        start_idx = 0
        while True:
            print('-'*80)
            print("\n")
            raw_data = input("Would you like to see 5 lines of raw data? Choose 'yes' or 'no': ").lower()
            if raw_data == 'yes':
                print(df.iloc[start_idx:start_idx+5])
                start_idx += 5
                print("\n")
            elif raw_data == 'no':
                print("\n")
                break
            else:
                print("Invalid input. Please Enter 'yes' or 'no'.")
                print("\n")
        
        # Ask the user if they want to restart the analysis
        restart = input("Would you like to restart the analysis? Enter 'yes' or 'no': ").lower()
        if restart != 'yes':
            break

if __name__ == "__main__":
    main()