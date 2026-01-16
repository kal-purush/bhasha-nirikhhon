package com.example.app.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.example.app.domain.BodyWeightTraining;

@Mapper
public interface BodyWeightTrainingMapper {

	//表示//
		//筋トレマシン一覧表示
		List<BodyWeightTraining> selectAll() throws Exception;

	//登録//
		//自重トレーニング記録登録
		void insert(BodyWeightTraining bwtRecord) throws Exception;
		
		
	//編集//
		//自重トレーニング記録編集
		void update(BodyWeightTraining bwtRecord) throws Exception;
		
	//削除//
		//自重トレーニング記録削除
		void delete(BodyWeightTraining bwtRecord) throws Exception;
}