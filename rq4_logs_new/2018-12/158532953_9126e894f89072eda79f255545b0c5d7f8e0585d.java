package com.jfspecial.modules.admin.maker.model;

import com.jfspecial.component.base.BaseProjectModel;
import com.jfspecial.jfinal.component.annotation.ModelBind;

@ModelBind(table = "tb_maker_signup")
public class TbMakerSignup extends BaseProjectModel<TbMakerSignup> {

	private static final long serialVersionUID = 1L;
	public static final TbMakerSignup dao = new TbMakerSignup();


}