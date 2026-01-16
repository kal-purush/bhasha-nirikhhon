
def max_temp_diff(filename):

    max_temp_diff = {}

    with open(filename, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:
            tokens = line.split(",")
            year = int(tokens[0])
            tmax = float(tokens[3])
            tmin = float(tokens[5])
            daily_temp_diff = tmax - tmin

            if year not in max_temp_diff or daily_temp_diff > max_temp_diff[year][0]:
               max_temp_diff[year] = (daily_temp_diff, f"{year}-{int(tokens[1]):02d}-{int(tokens[2]):02d}")

    return max_temp_diff


def cal_temperature(filename):
    cal_temp = {}  

    with open(filename) as f:
         lines = f.readlines()
         for line in lines[1:]:
            tokens = line.split(",")
            year = int(tokens[0])
            month = int(tokens[1])
            tavg = float(tokens[4])  # Assuming average temperature (you can adjust this based on your data)

            if 5 <= month <= 9:  # Consider only May to September
                if year in cal_temp:
                    cal_temp[year] += tavg
                else:
                    cal_temp[year] = tavg

    return cal_temp

if __name__ == '__main__':
    weather_filename = "hw12/weather(146)_2001-2022.csv"
    max_temp_diffs_years = max_temp_diff(weather_filename)
    accumulated_temperatures = cal_temperature(weather_filename)

    # 1번 최대일교차
    for year, (max_diff, date) in max_temp_diffs_years.items():
        print(f"{year}년의 최대일교차가 발생한 날짜는 {date}이며 {max_diff:.1f}°C 입니다")

    
    # 2번 5월부터 9월까지의 적산온도
    for year, temp in sorted(accumulated_temperatures.items()):
        print(f"{year}년의 5월부터 9월까지의 적산온도는 {temp:.1f}입니다.")