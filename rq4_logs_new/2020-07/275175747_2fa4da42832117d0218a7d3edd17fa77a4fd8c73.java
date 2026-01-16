package jm.stockx;

import jm.stockx.api.dao.RoleDAO;
import jm.stockx.entity.Role;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional
public class RoleServiceImpl implements RoleService {
    private final RoleDAO roleDAO;

    @Autowired
    public RoleServiceImpl(RoleDAO roleDAO) {
        this.roleDAO = roleDAO;
    }

    @Override
    public List<Role> getAllRoles() {
        return roleDAO.getAll();
    }

    @Override
    public Role getRoleById(Long id) {
        return roleDAO.getById(id);
    }

    @Override
    public void createRole(Role role) {
        roleDAO.add(role);
    }

    @Override
    public void deleteRole(Long id) {
        roleDAO.deleteById(id);
    }

    @Override
    public void updateRole(Role role) {
        roleDAO.merge(role);
    }
}