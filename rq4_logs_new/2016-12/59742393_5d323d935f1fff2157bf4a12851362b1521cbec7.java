package it.csi.frontend.iride.utils;

import java.util.List;

import org.geoserver.security.iride.entity.IrideIdentity;
import org.geoserver.security.iride.entity.IrideRole;

import com.google.common.collect.Lists;

import it.csi.sira.frontend.iride.vo.IrideIdentityVO;
import it.csi.sira.frontend.iride.vo.IrideRoleVO;

public class CastUtils {
	
	/**
    *
    * @param role
    * @return
    */
	public static IrideRoleVO getRoleVOFromRole(IrideRole role){
		IrideRoleVO vo = null;
		if(role != null){
			vo = new IrideRoleVO(role);
		}
		return vo;
	}

	/**
    *
    * @param id
    * @return
    */
	public static IrideIdentityVO getIrideIdentityVOFromIrideIdentity(IrideIdentity id){
		IrideIdentityVO vo = null;
		if(id != null){
			vo = new IrideIdentityVO(id.getCodFiscale(),id.getNome(),id.getCognome(),id.getIdProvider());
		}
		return vo;
	}
	
	/**
    *
    * @param roles
    * @return
    */
   public static IrideRoleVO[] getRolesVOFromRole(IrideRole[] roles) {
       final List<IrideRoleVO> roleVOs = Lists.newArrayList();
       for (final IrideRole role : roles) {
           roleVOs.add(getRoleVOFromRole(role));
       }
       return roleVOs.toArray(new IrideRoleVO[roles.length]);
   }
	
	
}