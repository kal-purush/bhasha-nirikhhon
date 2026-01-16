package org.chuxue.application.common.base;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestBody;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;

/**
 * 文件名 ： MybatisBaseConrollerImpl.java
 * 包 名 ： org.chuxue.application.common.base
 * 描 述 ： 通用coltroller
 * 机能名称：
 * 技能ID ：
 * 作 者 ： Administrator
 * 时 间 ： 2022年7月5日 上午9:46:15
 * 版 本 ： V1.0
 */

public class MybatisBaseConrollerImpl<T> implements BaseController<T> {

	private static final Logger	logger	= LoggerFactory.getLogger(BaseControllerImpl.class);
	
	@Autowired
	MybatisBaseServiceImpl<T>	mybatisBaseServiceImpl;
	
	/**
	 * 方法名 ： page
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param vo
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#page(org.chuxue.application.common.base.Pagination)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<Page<T>> page(@RequestBody Pagination<T> vo) {
		logger.info("<findAll> param vo:{} ", vo.toString());
		try {
			IPage<T> p = new com.baomidou.mybatisplus.extension.plugins.pagination.Page<>(vo.getPageNumber(), vo.getPageSize());
			T info = searchParamTo(vo.getSearchList(), vo.getInfo());
			// 简单分页查询
			QueryWrapper<T> queryWrapper = new QueryWrapper<>(info);
			// TODO
			IPage<T> re = mybatisBaseServiceImpl.page(p, queryWrapper);
			Pageable able = PageRequest.of(vo.getPageNumber(), vo.getPageSize());
			PageImpl<T> result = new PageImpl<>(re.getRecords(), able, re.getTotal());

			return ResultUtil.success(result);
		} catch (Exception e) {
			logger.error("<findAll> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
		
	}
	
	/**
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 方法名： searchParamTo
	 * 功 能： TODO(这里用一句话描述这个方法的作用)
	 * 参 数： @param searchList
	 * 参 数： @param info
	 * 参 数： @return
	 * 返 回： T
	 * 作 者 ： Administrator
	 * @throws
	 */
	private T searchParamTo(List<SearchParameters> searchList, T info) throws InstantiationException, IllegalAccessException {
//		Class<T> class1 = getClass();
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 方法名 ： findAll
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param info
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#findAll(org.chuxue.application.common.base.BaseEntity)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<List<T>> findAll(@RequestBody T info) {
		logger.info("<findAll> param info:{} ", info.toString());
		try {
			// 简单分页查询
			QueryWrapper<T> queryWrapper = new QueryWrapper<>(info);

			List<T> re = mybatisBaseServiceImpl.list(queryWrapper);
			return ResultUtil.success(re);
		} catch (Exception e) {
			logger.error("<findAll> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名： colName
	 * 功 能： TODO(这里用一句话描述这个方法的作用)
	 * 参 数： @param name
	 * 参 数： @return
	 * 返 回： String
	 * 作 者 ： Administrator
	 * @throws
	 */
	private String colName(String name) {
		StringBuilder result = new StringBuilder();
		if (name != null && name.length() > 0) {
			// 将第一个字符处理成大写
			result.append(name.substring(0, 1).toLowerCase());
			// 循环处理其余字符
			for (int i = 1; i < name.length(); i++) {
				String s = name.substring(i, i + 1);
				// 在大写字母前添加下划线
				if (s.equals(s.toUpperCase()) && !Character.isDigit(s.charAt(0))) {
					result.append("_");
				}
				// 其他字符直接转成大写
				result.append(s.toLowerCase());
			}
		}
		return result.toString();
	}

	/**
	 * 方法名 ： findOne
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param info
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#findOne(org.chuxue.application.common.base.BaseEntity)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> findOne(@RequestBody T info) {
		logger.info("<findAll> param info:{} ", info.toString());
		try {
			// 简单分页查询
			QueryWrapper<T> queryWrapper = new QueryWrapper<>(info);

			T re = mybatisBaseServiceImpl.getOne(queryWrapper);
			return ResultUtil.success(re);
		} catch (Exception e) {
			logger.error("<findAll> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： save
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param info
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#save(org.chuxue.application.common.base.BaseEntity)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> save(@RequestBody T info) {
		logger.info("<save> param info:{} ", info.toString());
		try {
			boolean f = mybatisBaseServiceImpl.saveOrUpdate(info);
			if (f) {
				return ResultUtil.success();
			} else {
				logger.error("<save> error:{} ", "更新错误");
				return ResultUtil.error("更新错误");
			}
		} catch (Exception e) {
			logger.error("<save> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： saveAll
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param vo
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#saveAll(org.chuxue.application.common.base.Pagination)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> saveAll(@RequestBody Pagination<T> vo) {
		logger.info("<saveAll> param vo:{} ", vo.toString());
		try {
			boolean f = mybatisBaseServiceImpl.saveOrUpdateBatch(vo.getList());
			if (f) {
				return ResultUtil.success();
			} else {
				logger.error("<saveAll> error:{} ", "更新错误");
				return ResultUtil.error("更新错误");
			}
		} catch (Exception e) {
			logger.error("<saveAll> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： deleteAll
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param vo
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#deleteAll(org.chuxue.application.common.base.Pagination)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> deleteAll(@RequestBody Pagination<T> vo) {
		logger.info("<deleteAll> param vo:{} ", vo.toString());
		try {
			boolean f = mybatisBaseServiceImpl.removeBatchByIds(vo.getList());
			if (f) {
				return ResultUtil.success();
			} else {
				logger.error("<deleteAll> error:{} ", "更新错误");
				return ResultUtil.error("更新错误");
			}
		} catch (Exception e) {
			logger.error("<deleteAll> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： delete
	 * 功 能 ： TODO(这里用一句话描述这个方法的作用)
	 * 参 数 ： @param info
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#delete(org.chuxue.application.common.base.BaseEntity)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> delete(@RequestBody T info) {
		logger.info("<delete> param info:{} ", info.toString());
		try {
			boolean f = mybatisBaseServiceImpl.removeById(info);
			if (f) {
				return ResultUtil.success();
			} else {
				logger.error("<delete> error:{} ", "更新错误");
				return ResultUtil.error("更新错误");
			}
		} catch (Exception e) {
			logger.error("<delete> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： count
	 * 功 能 ： 按条件统计
	 * 参 数 ： @param info
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#count(org.chuxue.application.common.base.BaseEntity)
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<Long> count(@RequestBody T info) {
		logger.info("<count> param info:{} ", info.toString());
		try {
			// 简单分页查询
			QueryWrapper<T> queryWrapper = new QueryWrapper<>(info);
//			Class<? extends Object> class1 = info.getClass();
//			Field[] f = class1.getDeclaredFields();
//			Map<String, Object> m = new HashMap<>();
//			for (Field field : f) {
//				field.setAccessible(true);// 设置访问为true
//				if (field.get(info) != null) {
//					String colName = colName(field.getName());
//					boolean fieldHasAnno = field.isAnnotationPresent(TableField.class);
//					if (fieldHasAnno) {
//						TableField fieldAnno = field.getAnnotation(TableField.class);
//						String v = fieldAnno.value();
//						if (StringUtils.isNotBlank(v)) {
//							colName = v;
//						}
//					}
//					m.put(colName, field.get(info));
//				}
//			}
//			queryWrapper.allEq(m);
			
			Long l = mybatisBaseServiceImpl.count(queryWrapper);
			return ResultUtil.success(l);
		} catch (Exception e) {
			logger.error("<count> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}
	
	/**
	 * 方法名 ： trunc
	 * 功 能 ： 截断，mybatis中没有只能用remove替代，
	 * 参 数 ： @return
	 * 参 考 ： @see org.chuxue.application.common.base.BaseController#trunc()
	 * 作 者 ： Administrator
	 */

	@Override
	public BaseResult<T> trunc() {
		logger.info("<trunc>  ");
		try {
			mybatisBaseServiceImpl.remove(null);
			return ResultUtil.success();
		} catch (Exception e) {
			logger.error("<trunc> error:{} ", e.getMessage());
			return ResultUtil.error(e.getMessage());
		}
	}

}