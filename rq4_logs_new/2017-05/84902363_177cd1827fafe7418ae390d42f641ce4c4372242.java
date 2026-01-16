package org.dimigo.exception;

/**
 * <pre>
 * org.dimigo.exception
 *   |_Movie
 *
 * 1. 개요 :
 * 2. 작성일 : 2017. 5. 29.
 * <pre>
 *
 * @author : Seung
 * @version : 1.0
 */
public class Movie {
    private String title;
    private int limitAge;

    public Movie(String title, int limitage) {
        this.title = title;
        this.limitAge = limitage;
    }

    public String getTitle() {
        return title;
    }

    public int getLimitAge() {
        return limitAge;
    }

    public void buyTicket(int age) throws Exception {
//        try {
//
//            System.out.println(getTitle()+" 즐감하세요.");
//        }catch (Exception e){
            if(age < limitAge) {
                throw new Exception(getTitle() + "은/는 " +getLimitAge()+"세 이상 관람가입니다.");

            }else {
                System.out.println(title + " 즐감하세요.");
        }
    }
}