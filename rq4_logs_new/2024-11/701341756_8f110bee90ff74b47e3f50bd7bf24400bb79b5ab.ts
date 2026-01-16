import { getHistory } from "@/lib/utils";
import { useEffect, useState } from "react";

const usePlaylistHistory = () => {
	const [playlists, setPlaylists] = useState<string[]>([]);

	useEffect(() => {
		setPlaylists(getHistory());
	}, []);

	return [playlists, setPlaylists] as const;
};

export default usePlaylistHistory;