package com.example.app.service;

import java.time.LocalDateTime;
import java.util.List;

import com.example.app.domain.MachineSetCount;


public interface MachineSetCountService {

//表示//
	//筋トレ記録全表示
	List<MachineSetCount> getSelectAll() throws Exception;

	//カレンダーから特定の日の筋トレ記録表示
	List<MachineSetCount> getMachineSetCountDay() throws Exception;

//登録//
	//筋トレ記録登録
	void addMachineSetCount(MachineSetCount machineRecord) throws Exception;

//編集//
	//筋トレ記録編集
	void editMachineSetCount(MachineSetCount machineRecord) throws Exception;

//削除//
	//筋トレ記録削除
	void deleteMachineSetCount(LocalDateTime date) throws Exception;
}