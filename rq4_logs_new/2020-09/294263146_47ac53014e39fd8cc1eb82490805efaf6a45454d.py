import time
import pandas as pd
import numpy as np

CITY_DATA = { 'chicago': 'chicago.csv',
              'new york city': 'new_york_city.csv',
              'washington': 'washington.csv' }

def get_filters():
    """
    Asks user to specify a city, month, and day to analyze.

    Returns:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    """
    print('Hello! Let\'s explore some US bikeshare data!')
    # TO DO: get user input for city (chicago, new york city, washington). HINT: Use a while loop to handle invalid inputs
    while True:
        city = str(input('Which city would like to analyze the data? [Chicago, New York City, Washington]:')).lower()
        if city in CITY_DATA:
            break
        
    #To Understand what variable to filter
    filterby = str(input('Please select filters for data analysis [month, day, both, none]')).lower()
    month = 'all'
    day = 'all'
    if filterby in ['month', 'both']:
        # TO DO: get user input for month (all, january, february, ... , june)
        months = ['january', 'february', 'march', 'april', 'may', 'june']
        while True:
            month = str(input('Enter the month name to filter by [January, February, March, April, May or June]:')).lower()
            if month in months:
                break
    if filterby in ['day', 'both']:
        # TO DO: get user input for day of week (all, monday, tuesday, ... sunday)
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        while True:
            day = str(input('Enter day of the week to filter by [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]:')).title()
            if day in days:
                break

    print('-'*40)
    return city, month, day

def load_data(city, month, day):
    """
    Loads data for the specified city and filters by month and day if applicable.

    Args:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    Returns:
        df - Pandas DataFrame containing city data filtered by month and day
    """
    # load data file into a dataframe
    df = pd.read_csv(CITY_DATA[city])

    # convert the Start Time column to datetime
    df['Start Time'] = pd.to_datetime(df['Start Time'])

    # extract month and day of week from Start Time to create new columns
    df['month'] = df['Start Time'].dt.month
    df['day_of_week'] = df['Start Time'].dt.weekday_name

    # filter by month if applicable
    if month != 'all':
        # use the index of the months list to get the corresponding int
        months = ['january', 'february', 'march', 'april', 'may', 'june']
        month = months.index(month) + 1

        # filter by month to create the new dataframe
        df = df[df['month'] == month]

    # filter by day of week if applicable
    if day != 'all':
        # filter by day of week to create the new dataframe
        df = df[df['day_of_week'] == day.title()]

    return df


def time_stats(df):
    """Displays statistics on the most frequent times of travel."""

    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    # TO DO: display the most common month
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    most_common_month = df['month'].mode()[0]
    print('The most common month is {}.\n'.format(months[most_common_month].title()))

    # TO DO: display the most common day of week
    most_common_dayw = df['day_of_week'].mode()[0]
    print('The most common day of week is {}.\n'.format(most_common_dayw))

    # TO DO: display the most common start hour
    df['hour'] = df['Start Time'].dt.hour
    most_common_hour = df['hour'].mode()[0]
    print('The most common hour is {}.\n'.format(most_common_hour))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    # TO DO: display most commonly used start station
    m_c_s_station = df['Start Station'].mode()[0]
    print('The most common start station is {}.\n'.format(m_c_s_station))

    # TO DO: display most commonly used end station
    m_c_e_station = df['End Station'].mode()[0]
    print('The most common end station is {}.\n'.format(m_c_e_station))

    # TO DO: display most frequent combination of start station and end station trip
    df['Station Combination'] = df['Start Station'] + ' to ' + df['End Station']
    m_c_se_station = df['Station Combination'].mode()[0]
    print('The most common combination of start station and end station is {}.\n'.format(m_c_se_station))


    print("\nThis took %s seconds.\n" % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    # TO DO: display total travel time
    travel_time = df['Trip Duration'].sum()
    print('The total travel time is {} seconds.\n'.format(travel_time))

    # TO DO: display mean travel time
    mean_travel_time = df['Trip Duration'].mean()
    print('The mean travel time is {} seconds.\n'.format(mean_travel_time))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    # TO DO: Display counts of user types
    count_user_type = df['User Type'].value_counts()
    print('The count of user types is as follows.\n{}\n'.format(count_user_type))

    if 'Birth Year' in df.columns:
        # TO DO: Display counts of gender
        count_gender = df['Gender'].value_counts()
        print('The count of user types is as follows.\n{}\n'.format(count_gender))


        # TO DO: Display earliest, most recent, and most common year of birth
        earliest_birth_year = df['Birth Year'].min()
        most_recent_birth_year = df['Birth Year'].max()
        most_common_birth_year = df['Birth Year'].mode()[0]
        print('The earliest birth year is {}.\n'.format(earliest_birth_year))
        print('The most recent birth year is {}.\n'.format(most_recent_birth_year))
        print('The most common birth year is {}.\n'.format(most_common_birth_year))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)

def display_df(df):
    """Display DataFrame Data to user upon their request."""
    
    #Ask user if they would like to see the data
    user_perm = str(input("Would you like to see the data [yes/no]?")).lower()
    
    if user_perm == "yes":
        i = 0
        while True:
            print(df.iloc[i:i+5,:])
            user_perm_2 = str(input("Would you like to see more data [yes or no]?")).lower()
            if user_perm_2 == "no":
                break
            i += 5 #Updating the counter

def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)
        display_df(df)

        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()