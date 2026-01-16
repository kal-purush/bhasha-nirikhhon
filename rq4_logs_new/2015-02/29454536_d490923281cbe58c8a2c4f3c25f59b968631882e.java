package cn.wh.anyst.rest;

import java.io.IOException;

import javax.servlet.ServletRequest;

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import cn.wh.anyst.service.QiniuService;

import com.qiniu.api.auth.AuthException;

@RestController
@RequestMapping("/api/v1/qiniu")
public class QiniuRest {
	
	private static Logger logger = LoggerFactory.getLogger(QiniuRest.class);
	
	@Autowired
	private QiniuService qiniuService;
	
	@RequestMapping(value="/upload", method = RequestMethod.POST)
	public String uploadPics(
			@RequestParam("uploadFile") MultipartFile file,
			ServletRequest request) {
		try {
			return qiniuService.uploadFile("anyst", file.getInputStream());
		} catch (AuthException e) {
			logger.error(e.getStackTrace().toString());
			return null;
		} catch (JSONException e) {
			logger.error(e.getStackTrace().toString());
			return null;
		} catch (IOException e) {
			logger.error(e.getStackTrace().toString());
			return null;
		}
	}
}