package beans;

public class ReservationDto {
    private String id;
    private String lectureroomNum;
    private String startTime;
    private String endTime;
    private String seat;
    private String state;
    private String date;

    public static class Builder{
        private String id;
        private String lectureroomNum;
        private String startTime;
        private String endTime;
        private String seat;
        private String state;
        private String date;

        public Builder id(String id){
            this.id = id;
            return this;
        }

        public Builder lectureroomNum(String lectureroomNum){
            this.lectureroomNum = lectureroomNum;
            return this;
        }

        public Builder startTime(String startTime){
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(String endTime){
            this.endTime = endTime;
            return this;
        }

        public Builder seat(String seat){
            this.seat = seat;
            return this;
        }

        public Builder state(String state){
            this.state = state;
            return this;
        }

        public Builder date(String date){
            this.date = date;
            return this;
        }

        public ReservationDto build() {
            return new ReservationDto(this);
        }
    }

    public ReservationDto(){}

    public ReservationDto(Builder builder) {
        this.id = builder.id;
        this.lectureroomNum = builder.lectureroomNum;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.seat = builder.seat;
        this.state = builder.state;
        this.date = builder.date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLectureroomNum() {
        return lectureroomNum;
    }

    public void setLectureroomNum(String lectureroomNum) {
        this.lectureroomNum = lectureroomNum;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getSeat() {
        return seat;
    }

    public void setSeat(String seat) {
        this.seat = seat;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}