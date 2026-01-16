package servise;

import model.Role;
import repository.RoleRepository;

import java.util.List;
import java.util.Optional;


public class RoleServiceImpl implements RoleService {

    private final RoleRepository roleRepository;

    public RoleServiceImpl(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }

    @Override
    public Optional<Role> getRoleByName(String name) {
                // Поиск по имени роли, например, ADMIN или USER
        return roleRepository.findByName(name.toUpperCase());
    }

    @Override
    public void saveRole(String name) {
        // Используем перечисление для создания роли
        Role role = Role.valueOf(name.toUpperCase());
        roleRepository.save(role);
    }

    @Override
    public void deleteRole(int id) {
        roleRepository.deleteById(id);
    }

    @Override
    public List<Role> getAllRoles() {
        return roleRepository.findAll();
    }
}