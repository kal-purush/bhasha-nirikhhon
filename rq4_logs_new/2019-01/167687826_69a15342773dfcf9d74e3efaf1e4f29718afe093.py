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
    # gets user input for city (chicago, new york city, washington)
    while True:
        cities = ['chicago', 'new york city', 'washington']
        city = input('\nWould you like to see data from Chicago, New York City, or Washington?\n').lower()
        if city not in cities:
            print('\nERROR: Please enter a valid city!')
        else:
            break

    # gets user input for month (all, january, february, ... , june)
    while True:
        months = ['all', 'january', 'february', 'march', 'april', 'may', 'june']
        month = input('\nFilter by January, February, March, April, May, June, or All?\n').lower()
        if month not in months:
            print('\nERROR: Please enter a valid month!')
        else:
            break

    # gets user input for day of week (all, monday, tuesday, ... sunday)
    while True:
        day_of_week = ['all', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        day = input('\nFilter by Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday, or All?\n').lower()
        if day not in day_of_week:
            print('\nERROR: Please enter a valid day of the week!')
        else:
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
    df['Month'] = df['Start Time'].dt.month
    df['Day of Week'] = df['Start Time'].dt.weekday_name

    # filter by month if applicable
    if month != 'all':
        # use the index of the months list to get the corresponding int
        months = ['january', 'february', 'march', 'april', 'may', 'june']
        month = months.index(month) + 1

        # filter by month to create the new dataframe
        df = df[df['Month'] == month]

    # filter by day of week if applicable
    if day != 'all':
        # filter by day of week to create the new dataframe
        df = df[df['Day of Week'] == day.title()]

    return df


def time_stats(df):
    """Displays statistics on the most frequent times of travel."""

    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    # displays the most common month
    common_month = df['Month'].mode()[0]
    print('Most popular month: {}'.format(common_month))

    # displays the most common day of week
    common_day_of_week = df['Day of Week'].mode()[0]
    print('Most popular day: {}'.format(common_day_of_week))

    # displays the most common start hour
    common_hour = df['Start Time'].dt.hour.mode()[0]
    print('Most popular start hour: {}'.format(common_hour))
    

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    # displays most commonly used start station
    common_start_station = df['Start Station'].mode()[0]
    print('Start station: {}'.format(common_start_station))
    
    # displays most commonly used end station
    common_end_station = df['End Station'].mode()[0]
    print('End station: {}'.format(common_end_station))
    
    # displays combinations of start station and end station trip
    combination = df.groupby(['Start Station', 'End Station']).size().reset_index(name='Count')
    # dataframe ct (abbv. common trip) holds row where the count of the combination equals the max of trips
    ct = combination.loc[combination['Count'] == combination['Count'].max()]
    # displays most frequent combination and amount of times trip was taken
    print('Most popular trip: {} >> {}, taken {} time(s)'.format(ct.iloc[0]['Start Station'], ct.iloc[0]['End Station'], ct.iloc[0]['Count']))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    # displays total travel time in seconds
    total_travel = np.sum(df['Trip Duration'])
    print('Total travel time (seconds): {}'.format(total_travel))

    # displays mean travel time in seconds
    mean_travel = np.mean(df['Trip Duration'])
    print('Average travel time (seconds): {}'.format(mean_travel))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    # displays counts of user types
    user_types = df['User Type'].value_counts()
    print('Subscribers: {}'.format(user_types['Subscriber']))
    print('Customers: {}'.format(user_types['Customer']))

    # this block is fully executed if 'Gender' column exists
    try:
        # displays counts of gender
        gender = df['Gender'].value_counts()
        print('Male users: {}'.format(gender['Male']))
        print('Female users: {}'.format(gender['Female']))        
    except KeyError as e:
        print("{} column doesn't exist in this dataset!".format(e))   
        
    # this block is fully executed if 'Birth Year' column exists
    try:
        # displays earliest and most recent birth year
        early_birth = df['Birth Year'].min()
        print('Earliest birth year: {}'.format(int(early_birth)))
        recent_birth = df['Birth Year'].max()
        print('Recent birth year: {}'.format(int(recent_birth)))
        
        # creates dataframe with only the birth year and the amount of times the given birth year appears
        births = df.groupby('Birth Year')['Birth Year'].count().reset_index(name='Count')
        # dataframe cb (abbv. common birth) holds the row where the count of the births equals the most births in a single year
        cb = births.loc[births['Count'] == births['Count'].max()]
        # displays most common birth year and the amount of births that occurred in that year
        print('Most common birth year: {}, {} birth(s)'.format(int(cb.iloc[0]['Birth Year']), int(cb.iloc[0]['Count'])))       
    except KeyError as e:
        print("{} column doesn't exist in this dataset!".format(e))        

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)

    
def display_raw_data(df):
    """Asks user to view raw data, displays five lines of data at a time."""
    idxa, idxb = (0, 5)
    # removes the added columns from load_data function in order to present original dataframe
    df = df.drop(['Month', 'Day of Week'], axis=1)
    
    view_data = input('\nWould you like to view the raw data? Enter yes or no.\n')
    while True:
        if view_data.lower() == 'yes':
            # displays lines of dataframe within given indices
            print('\n{}'.format(df.iloc[idxa:idxb]))
            idxa += 5
            idxb += 5
        else:
            break
        print('-'*40)
        view_data = input('\nView next five lines? Enter yes or no.\n')

    print('-'*40)
          
        
def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)
        display_raw_data(df)

        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()