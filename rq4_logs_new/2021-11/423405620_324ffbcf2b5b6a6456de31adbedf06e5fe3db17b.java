package ruangong.our_land.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ruangong.our_land.model.ResultInfo;
import ruangong.our_land.model.spirit.Spirit;
import ruangong.our_land.model.spirit.boss.Boss;
import ruangong.our_land.model.spirit.monster.Monster;
import ruangong.our_land.model.user.UserSpirit;
import ruangong.our_land.service.SpiritService;
import ruangong.our_land.service.UserService;

import javax.annotation.Resource;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author hwy
 * @Date 2021/11/26
 */

@RestController
@RequestMapping("/spirit")
public class SpiritController {

    @Resource
    private SpiritService spiritService;

    @Resource
    private UserService userService;

    @PostMapping("/getInfo")
    public ResultInfo getBattleInfo(@RequestParam("userId") @Valid Integer uId,
                                    @RequestParam("bossId") @Valid Integer bossId){
        Boss bossInfo = spiritService.findBoss(bossId);
        List<Integer> sIdList = userService.findSpirit(uId).stream().map(UserSpirit::getS_id).collect(Collectors.toList());
        List<Monster> userInfo = new ArrayList<>();
        for (Integer sId : sIdList) {
            userInfo.add(spiritService.findMonster(sId));
        }
        Map<String, Object> map = new HashMap<>();
        map.put("bossInfo", bossInfo);
        map.put("userInfo", userInfo);
        return ResultInfo.success(map);
    }

}