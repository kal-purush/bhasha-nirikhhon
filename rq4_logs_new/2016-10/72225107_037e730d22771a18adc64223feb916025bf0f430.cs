class Hourly : Employee
    {
        private double hourlyPay;
        private double hoursWorked;
        private double totalHoursWorked;
        private DateTime periodStarted;
        private DateTime periodEnded;
        private String reportCalendar;
        private double periodPaymentAmount;
        private String report;

        public Hourly()
        {
            hourlyPay = 7.25;
            hoursWorked = 0;
            periodStarted = DateTime.Now;
            periodEnded = DateTime.Now;
            periodPaymentAmount = 0;
        }

        public double HourlyPay
        {
            get
            {
                return hourlyPay;
            }

            set
            {
                hourlyPay = value;
            }
        }

        public double HoursWorked
        {
            get
            {
                return hoursWorked;
            }

            set
            {
                hoursWorked = value;
                totalHoursWorked += hoursWorked;
                calculatePayment();
            }
        }

        public double PeriodPaymentAmount
        {
            get
            {
                return periodPaymentAmount;
            }

            set
            {
                periodPaymentAmount = value;
            }
        }

        public double calculatePayment()
        {
            return periodPaymentAmount =  hourlyPay * totalHoursWorked;
        }

        public void todayReport()
        {
            report += DateTime.Now + " : " + hoursWorked;
        }

        public void addTime(double timeWorked)
        {
            reportCalendar += DateTime.Now + " : " + timeWorked;
        }

        public void payEmployee()
        {
            //ToDo
            //1- check that the periodPayment Amount is different than 0
            //2- Create an object payment
            //3- Base on the payment method choose a payment function.
        }
   }