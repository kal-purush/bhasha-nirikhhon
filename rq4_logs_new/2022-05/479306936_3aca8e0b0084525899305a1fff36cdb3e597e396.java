package wiseSWlife.wiseSWlife.domain.auction;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Data
@Getter @Setter
public class Auction {

    //    판매 번호
    private Long auctionSeq;
    //    판매자
    private String seller;
    //    상품 이름
    private String productName;
    //    상품 가격
    private Long price;
    //    상품 설명
    private String Text;
    //    상품 이미지 경로
    private String imgUrl;
    //    판매 시간
    private Date date;
    //    판매완료 여부
    private boolean soldOut;

    public Auction(String seller, String productName, Long price, String text, Date date) {
        this.seller = seller;
        this.productName = productName;
        this.price = price;
        Text = text;
        this.date = date;
    }
}