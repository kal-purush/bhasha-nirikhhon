package com.example.demo.taskManeger.entity;

import java.sql.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name="task")
public class Task {
	// データベースに保存する項目名
	@Id
	private Integer id;
	//タスク名
	private String task;
	//期日
	private Date date;
//	//分類
//	private String sort;
	//完了
	private boolean completion;
}