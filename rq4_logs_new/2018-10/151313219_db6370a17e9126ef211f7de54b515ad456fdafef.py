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
    city=''
    while city=='':
          city_key_in=input('Please say what city you want to see: Chicago, New York City or Washington? ').lower()
          if city_key_in in ['chicago', 'new york city', 'washington']:
             city=city_key_in
             break
          else: 
              print('Invalid key in and please try again')
    print('You just chose {}.'.format(city))
           
    # TO DO: get user input for month (all, january, february, ... , june)
    month=''
    while month=='':
          month_key_in=input('Please say if you want to see all or want to filter on a month (use month full name like January, February,etc)? ').lower()
          if month_key_in in ['january', 'february', 'march', 'april', 'may', 'june', 'all']:
             month=month_key_in           
             break
          else:
             print('Please key in the right month or key in all')
    print('You just chose {}.'.format(month))
    
    # TO DO: get user input for day of week (all, monday, tuesday, ... sunday)
    day=''
    while day=='':
          day_key_in=input('Please say if you want to see all or want to filter on a day (use weekday name like Monday,Tuesday,etc)? ').lower()
          if day_key_in in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday','sunday','all']:
             day=day_key_in           
             break
          else:
             print('Please key in the right weekday name or key in all')
    print('You just chose {}.'.format(day))
    
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
    # load data according to the input
    df = pd.read_csv(CITY_DATA[city])
    # convert the Start Time column to datetime
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    #get the month number from start time column and store in a new column - month
    df['month'] = df['Start Time'].dt.month
    #similar, create the week day name column
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
    # only run when filter is in all month
    if df['month'].nunique()!=1:
       month_number= df['month'].mode()[0]
       months = ['January', 'February', 'March', 'April', 'May', 'June']
       popular_month=months[month_number-1]
       print('The most common month is: ', popular_month)
   
    # TO DO: display the most common day of week
    # only run when filter is in all days
    if df['day_of_week'].nunique()!=1:
       popular_weekday= df['day_of_week'].mode()[0]
       print('The most common day of week is: ', popular_weekday)
  
    # TO DO: display the most common start hour;run anytime when have input
    df['hour'] =df['Start Time'].dt.hour
    popular_hour = df['hour'].mode()[0]
    print('the most common start hour is: ', popular_hour)
   
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    # TO DO: display most commonly used start station
    common_start=df['Start Station'].mode()[0]
    print('The most commonly used start station is: ', common_start)
    # TO DO: display most commonly used end station
    common_end=df['End Station'].mode()[0]
    print('The most commonly used end station is: ', common_end)
    # TO DO: display most frequent combination of start station and end station trip
    df['common_trip']=df['Start Station'] + '  TO  ' + df['End Station']
    combi=df['common_trip'].mode()[0]
    print('The most frequent combination of sthart station and end station trip is: ', combi)

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    # TO DO: display total travel time
    # convert end time column to datetime format
    df['End Time'] = pd.to_datetime(df['End Time'])
    #caculate the duration
    df['Duration']=df['End Time']-df['Start Time']
    # get the total travel time
    total_time=df['Duration'].sum()
    #get the mean travel time
    mean_time=df['Duration'].mean()
    # TO DO: display total and mean travel time
    print('The total travel time in the database is: ',total_time)
    print('The average travel time in the database is: ',mean_time)

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    # TO DO: Display counts of user types
    user_types = df['User Type'].value_counts()
    print('counts of user types :')
    print(user_types)

    # TO DO: Display counts of gender, only chicago and new york city
    try:
        user_gender_types=df['Gender'].value_counts()
        print('counts of gender types: ')
        print(user_gender_types)
    except KeyError:
        print('This city does not have gender information.')
    # TO DO: Display earliest, most recent, and most common year of birth
    try:
    # only available for new york city and chicago; first get earliest year
        min_year=df['Birth Year'].min()
    # most recent year
        max_year=df['Birth Year'].max()
    # most common year
        common_year=df['Birth Year'].mode()[0]
        year_list=[min_year,max_year,common_year]
        print('The earliest, most recent and most common year of birth are: ',year_list)
    except KeyError:
        print('This city does not have birth year information.')

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)
        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)

        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()