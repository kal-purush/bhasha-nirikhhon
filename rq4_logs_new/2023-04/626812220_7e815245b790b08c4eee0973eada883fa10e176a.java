package com.billi.vo;

import java.sql.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Setter@Getter@ToString
public class BoardsVO {
	private int board_id;
	private String board_title;
	private String board_contents;
	private String board_writer;
	private Date board_date;
	private int price;
	private String pictures;
	private String address;
	private String category;
}