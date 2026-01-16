package com.stupidwind.blog.exception;

import com.stupidwind.blog.consts.ResultConst;
import com.stupidwind.blog.domain.vo.ResultBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author: StupidWind
 * @date: 2019/9/21 11:20
 * @description: 异常处理器
 * @ver: 1.0.0
 */
@Slf4j
@ControllerAdvice
public class ExceptionResolver {

	@ResponseBody
	@ExceptionHandler(Exception.class)
	public ResultBean handleException(Exception ex) {
		ResultBean result = new ResultBean();
		if (ex instanceof Exception) {
			result.setCode(ResultConst.RESULT_FAILURE);
			result.setMsg("操作失败");
			result.setSuccess(false);
		}
		return result;
	}

}