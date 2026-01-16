package com.transteven.multiplaylist.content.youtube.controller;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;

import com.transteven.multiplaylist.content.youtube.model.YoutubeModel;
import com.transteven.multiplaylist.content.youtube.service.YoutubeService;

@RestController
@RequestMapping("/api")
public class YoutubeController {
	@Autowired
        YoutubeService ytService;

		@RequestMapping(value="/playlist", method=RequestMethod.POST)
		public YoutubeModel createYoutube(@RequestBody YoutubeModel yt) {
			return ytService.createYoutube(yt);
		}


		@RequestMapping(value="/playlist", method=RequestMethod.GET)
			public List<YoutubeModel> readYoutube() {
    		return ytService.getYoutube();
		}


		@RequestMapping(value="/playlist/{ytId}", method=RequestMethod.PUT)
			public YoutubeModel readYoutube(@PathVariable(value = "ytId") Long id, @RequestBody YoutubeModel ytDetails) {
    		return ytService.updateYoutube(id, ytDetails);
		}


		@RequestMapping(value="/playlist/{ytId}", method=RequestMethod.DELETE)
			public void deleteYoutube(@PathVariable(value = "ytId") Long id) {
		    ytService.deleteYoutube(id);
		}
}