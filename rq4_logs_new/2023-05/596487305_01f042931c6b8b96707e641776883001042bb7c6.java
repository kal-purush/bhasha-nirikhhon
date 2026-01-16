package it.project1.monster;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("monsters")
public class MonsterController {
    private final MonsterService monsterService;

    public MonsterController(MonsterService monsterService) {
        this.monsterService = monsterService;
    }
    @GetMapping
    public List<MonsterEntity> getAllMonsters() {
        return monsterService.getAllMonsters();
    }

    @GetMapping("/{id}")
    public MonsterEntity getMonsterById(@PathVariable Long id) {
        return monsterService.getMonsterById(id);
    }

    @PostMapping
    public MonsterEntity createMonster(@RequestBody MonsterEntity monster) {
        return monsterService.createOrUpdateMonster(monster);
    }

    @PutMapping("/{id}")
    public MonsterEntity updateMonster(@PathVariable Long id, @RequestBody MonsterEntity monster) {
        monster.setMonsterId(Math.toIntExact(id));
        return monsterService.createOrUpdateMonster(monster);
    }

    @DeleteMapping("/{id}")
    public void deleteMonster(@PathVariable Long id) {
        monsterService.deleteMonster(id);
    }
}