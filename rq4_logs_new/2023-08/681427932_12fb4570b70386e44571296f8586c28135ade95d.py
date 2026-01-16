import statistics as stats

data_a = {10, 11, 14, 20, 22, 24, 28, 31}

mean_a = stats.mean(data_a)
median_a = stats.median(data_a)
mode_a = stats.mode(data_a)
variance_a = stats.pvariance(data_a)

print("Mean of data a = " , mean_a)
print("Median of data a = " , median_a)
print("Mode of data a = " , mode_a)
print("Variance of data a = " , variance_a)

data_b= {2, 9, 13, 14, 20, 20, 24, 26, 32, 40}

mean_b = stats.mean(data_b)
median_b = stats.median(data_b)
mode_b = stats.mode(data_b)
variance_b = stats.pvariance(data_b)

print("Mean of data b = " , mean_b)
print("Median of data b = " , median_b)
print("Mode of data b = " , mode_b)
print("Variance of data b = " , variance_b)