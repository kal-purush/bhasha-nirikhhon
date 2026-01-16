package org.example.homework_nr_6up;

public class Date {
        public static void main(String[] args) {
            Date Data1 = new Date(12,03,2003);
            System.out.println(Data1.displayDate());
            Data1.setDay(-1);
            Data1.setMonth(13);
            Data1.setYear(2023);
            System.out.println(Data1.displayDate());
        }
        private int Day;
        private int Month;
        private int Year;

        public Date(int Day, int Month, int Year) {
            this.Day = Day;
            this.Month = Month;
            this.Year = Year;
        }

        public void setDay(int Day) {
            if (Day>=1 && Day <=30)
                this.Day = Day;
            else System.out.println("This day is invalid "+ Day);
        }

        public void setMonth(int Month) {
            if (Month >=1 && Month<=12)
                this.Month = Month;
            else System.out.println("This month is invalid "+ Month);
        }

        public void setYear(int Year) {

            if(Year>0)
                this.Year = Year;
        }

        public String displayDate () {
            return this.Day + "/" + this.Month + "/" + this.Year;
        }


    }