public static class BoardStatus
{
    /**Mới đầu sẽ ở trạng thái này
	 * Khi join vào bàn:
	 * 	Nếu bàn đang không chơi
	 * 		=> WAITING
	 * 	Nếu bàn đang chơi thì vẫn để là NOT_INIT
	 * (để boardModel.isPlaying == true, để boardLogic.updatePlayers thực hiện đúng) 
	 * 	Khi đó, server sẽ gửi về ResumeVO (@see ResumeBoardCommand)
	 * 		=> CHIA_BAI
	 * 		rồi => các status khác dựa vào ResumeVO đã có những action nào rồi*/
	public const int NOT_INIT = -1;
	
	/**đang chờ người vào chơi
	 * khi thằng chia bài chọn bắt đầu (@see ReceivedChiaBaiCommand)
	 * 		=> CHIA_BAI 
	 * (thằng chia bài cũng là thằng chọn nọc, bốc cái. Là thằng thắng ván trước hoặc là chủ bàn)
	 * @see issue #38, #101*/
	public const int WAITING = 0;
	
	/**đã chọn bắt đầu (đã chia bài)
	 * Khi có thằng thoát khi đang chia bài: UserExitedBoardCommand
	 * 		=> SUMUP (SumUpCommand)
	 * //khi cả làng thoát: UserExitedBoardCommand
	 * //		=> SUMUP (SumUpCommand)//Thực ra vẫn => WAIT_U rồi => SUMUP luôn
	 * (khi thằng chia bài chọn bài nọc thì vẫn ở status CHIA_BAI)
	 * khi thằng chia bài bốc cái (chọn bài cái thì bốc cái luôn)
	 * 		=> BOC_CAI (SDExtEvent.EVENT_BOC_CAI)
	 * @see issue #38.
	 * 10s mà không chọn nọc & bốc cái thì tự động chọn nọc & bốc cái luôn
	 * (tự chọn nọc & bốc cái nghĩa là tự gửi thông tin chọn nọc, rồi gửi luôn thông tin bốc cái)
	 * Migrating note: Ngay khi chia bài sẽ shuffle cards & send cho làng luôn (move GiveCardsVO.cards to StartVO)*/
	public const int CHIA_BAI = 1;

	/**
	 * Fixes #355
	 * Sau khi bốc cái và chưa bắt đầu chơi.
	 * Khi bài chia xong, tất cả các thằng đều nhận được bài: GiveCardsCompleteCommand
	 * => PLAYING
	 */
	public const int BOC_CAI = 2;

	/**đang chơi
	 * Khi cả làng báo/thoát hết: UserExitedBoardCommand, ClockTimeOutCommand
	 * 		=> WAIT_U (StopPlayCommand) rồi => SUMUP (SumUpCommand) luôn
	 * 	Note:
	 * 	 + Tại PlayReceivedCommand, nếu check thấy thằng đang chơi báo thì cũng k làm gì, vẫn clock.startClock như thường
	 * 	   Nhưng tại Clock.startClock, nếu thấy đếm giờ cho thằng báo | dis thì timeOut luôn
	 * 	   Nên logic đc làm ở ClockTimeOutCommand
	 * 	 + Nếu cả làng thoát hết thì chuyển luôn sang SUMUP & k gửi CMD_SUMUP
	 * 	   Còn nếu hòa hoặc cả làng báo/thoát nhưng vẫn còn player chưa thoát thì vẫn gửi CMD_SUMUP
	 * 
	 * Ngay sau khi Bốc quân thứ 22 trong nọc: PlayReceivedCommand
	 * 		=> WAIT_U (StopPlayCommand)
	 * 
	 * Khi nhận được UVO: UReceivedCommand
	 * 	Tức khi có thằng nào đó - tính cả mình - đã chọn quân ù. 
	 * 	Tính cả trường hợp thiên ù hay tự ù, hay ù láo, ù báo, treo tranh, bỏ ù
	 * 		=> WAIT_U (StopPlayCommand)*/
	public const int PLAYING = 3;
	
	/**Không "chơi" được nữa: Các action định nghĩa trong PlayVO đều bị disable.
	 * Lúc này chỉ có nút Ù là sáng.
	 * 
	 * WAIT_U luôn chuyển sang SUMUP (SumUpCommand).
	 * Nhưng có trường hợp chuyển luôn, có trường hợp phải chờ thời gian xem có ai ù không. @see PLAYING
	 * 
	 * Khi cả làng báo/thoát => chuyển luôn
	 * 
	 * Khi hết nọc | nhận đc UVO => chờ cho đến khi CẢ LÀNG THẤY canUCards = empty (không còn quân nào trên bàn có thể ù)
	 * thì sẽ CheckSumUpCommand. Tại đó sẽ biết được ván kết thúc do hòa, hay do ù,
	 * và nếu là ù thì biết được thằng nào được ưu tiên nhất rồi mới chuyển => SUMUP
	 * Để đảm bảo CẢ LÀNG THẤY: @see CanUCardsModel.startCheckSumUpTimer*/
	public const int WAIT_U = 4;
	
	/**đã tổng kết ván, đang chờ stop ván để prepare cho ván mới
	 * Khi nhận đc CMD_STOP
	 * 		=> WAITING
	 * 	//Nếu kết thúc do chủ bàn đang chia thì thoát, hoặc khi cả làng thoát -> send(CMD_STOP)
	 * 	//Khi hòa hoặc cả làng thoát hoặc báo & có người chưa thoát thì:
	 * 		delayedCall: send(CMD_STOP)
	 * 		nếu mình là player (k phải spectator): send(CMD_SUMUP, DRAW)*/
	public const int SUMUP = 5;
	public const int SUM_UPPED = 6;
}