import time
import pandas as pd
import numpy as np

CITY_DATA = { 'chicago': 'chicago.csv',
              'new york city': 'new_york_city.csv',
              'washington': 'washington.csv' }

def get_filters():
    # gets the specifications the user wants data for
    print('\nHello! Let\'s explore some US bikeshare data!\n')  
    city = input("For which city would you like to see data from? Chicago, New York City or Washington? ").lower()
    
    while city not in ['chicago', 'washington', 'new york city']:
        print('\nInvalid input.\n')
        city = input('Please select one of these cities: Chicago, Washington or New York City? ').lower()
    
    month = input("\nWhich month? January, February, March, April, May, June or all? \n" ).lower()

    while month not in['january', 'february', 'march', 'april', 'may', 'june', 'all']:
        print('\nInvalid input\n')
        month = input('Please enter the month you selected again. ')

    day = input("Select a day of the week or you can  type all for no day filter. \n" ).lower()

    while day not in['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'all']:
        print('\nInvalid input\n')
        day = input ("Please try again. ")

    print('-'*40)
    return city, month, day


def load_data(city, month, day):
     # loads data for specified city, month, day 
    df = pd.read_csv(CITY_DATA[city])
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month'] = df['Start Time'].dt.month
    df['day_of_week'] = df['Start Time'].dt.weekday_name
    df['hour'] = df['Start Time'].dt.hour
    
    if month != 'all':
        months = ['january', 'february', 'march', 'april', 'may', 'june']
        month = months.index(month) + 1
        df = df[df['month'] == month]
        
    if day != 'all':
        df = df[df['day_of_week'] == day.title()]
        
    return df

def time_stats(df):
    
    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    popular_month = df.loc[:, 'month'].mode()[0]
    popular_day = df.loc[:, 'day_of_week'].mode()[0]
    popular_hour = df.loc[:, 'hour'].mode()[0]
                      
    print(' Most frequent month: {} \n Most frequent day: {} \n Most frequent hour: {}'.format(popular_month, popular_day, popular_hour))
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    popular_start_station = df.loc[:, 'Start Station'].mode()[0]
    popular_end_station = df.loc[:, 'End Station'].mode()[0]
    df['Start End Combo'] = df['Start Station'] + df['End Station']
    popular_start_end_combo = df.loc[:, 'Start End Combo'].mode()[0]

    print(' Most popular start station: {} \n Most popular end station: {} \n Most popular trip: {}'.format (popular_start_station, popular_end_station, popular_start_end_combo))
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
  
    print('\nCalculating Trip Duration...\n')
    start_time = time.time()
    total_duration = df.loc[:, 'Trip Duration'].sum()
    mean_duration = df.loc[:, 'Trip Duration'].mean()
    
    print(' The total trip duration: {} \n The average trip duration: {}'.format(total_duration, mean_duration))
    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    
    try:
        print('\nCalculating User Stats...\n')
        start_time = time.time()

        user_type_count = df['User Type'].value_counts()
        gender_count = df['Gender'].value_counts()
        min_year = df['Birth Year'].min()
        max_year = df['Birth Year'].max()
        common_year = df['Birth Year'].mode()

        print(user_type_count, gender_count, min_year, max_year, common_year)

        print("\nThis took %s seconds." % (time.time() - start_time))
        print('-'*40)
    except KeyError:
        print(" User stats not availible for this city.")

def display_raw_data(df):
    i = 0
    raw = input("\nWould you like to see the first 5 rows of data: type 'yes' or 'no'?\n").lower()
    pd.set_option('display.max_columns',200)
    
    while True:
        if raw == 'no':
            break
        print(df[i:i+5])
        raw = input('\nWould you like to see the next rows of raw data?\n').lower()
        i += 5
        
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