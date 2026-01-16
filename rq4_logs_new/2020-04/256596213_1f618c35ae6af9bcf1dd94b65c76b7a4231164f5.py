from django.db import models

# Create your models here.
class StockInfo(models.Model):
    st_name = models.CharField(max_length = 30)
    def __str__(self):
        return self.st_name

class DayData(models.Model):
    Data = models.DateField()
    Open = models.FloatField()
    High = models.FloatField()
    Low = models.FloatField()
    Close = models.FloatField()
    AdjClose = models.FloatField()
    Volume = models.FloatField()
    stockinfo = models.ForeignKey(StockInfo, on_delete = models.CASCADE)
    def __str__(self):
        return self.Data,self.Close

class MonthData(models.Model):
    Data = models.DateField()
    Open = models.FloatField()
    High = models.FloatField()
    Low = models.FloatField()
    Close = models.FloatField()
    AdjClose = models.FloatField()
    Volume = models.FloatField()
    stockinfo = models.ForeignKey(StockInfo, on_delete=models.CASCADE)

    def __str__(self):
        return self.Data, self.Close


class RealTimeData(models.Model):
    t_time = models.TimeField()
    real_price = models.FloatField()
    stockinfo = models.ForeignKey(StockInfo,on_delete = models.CASCADE)
    def __str__(self):
        return self.t_time,self.real_price
