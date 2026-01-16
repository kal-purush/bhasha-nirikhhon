import { useEffect } from "react";
import { useTimeAttackStore } from "../../../store/useTimeAttackStore";
import { useUserStore } from "../../../store/useUserStore";
import { useSocketRefStore } from "../../../store/useSocketRefStore";

const gameDuration = 11;

export const usePlayTimeAttack = () => {
	//   const navigate = useNavigate();
	const setClearTime = useTimeAttackStore((state) => state.setClearTime);
	const { name: userName } = useUserStore();
	const time = useSocketRefStore((state) => state.eventState.time);

	//   timeが0になったらrankingページへ遷移する
	useEffect(() => {
		if (time - 1 === gameDuration) {
			//   navigate("/ranking_timeAttack");
			setClearTime(18);
		}
	}, [time, setClearTime]);

	return { time, userName };
};