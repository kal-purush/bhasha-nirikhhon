package com.zhang.libweb.model.book;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import com.zhang.libweb.model.BaseDTO;
import lombok.Data;

/**
 * book
 * @author 
 */
@Data
public class BookDTO extends BaseDTO implements Serializable {
    /**
     * 书籍名称
     */
    private String bookName;

    /**
     * 分类主键
     */
    private Integer bookClassId;

    /**
     * 书籍价格
     */
    private BigDecimal bookPrice;

    /**
     * 出版社名称
     */
    private String bookPublish;

    /**
     * 作者
     */
    private String bookAuthor;

    /**
     * 书籍图片(img1;img2)
     */
    private String bookImg;

    /**
     * 出版日期(yyyy-MM-dd)
     */
    private Date publishDate;

    private static final long serialVersionUID = 1L;
}