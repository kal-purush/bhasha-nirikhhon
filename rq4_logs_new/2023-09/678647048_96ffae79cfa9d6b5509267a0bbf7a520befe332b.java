package net.okuri.qol;

import org.bukkit.Bukkit;
import org.bukkit.NamespacedKey;
import org.bukkit.entity.Player;
import org.bukkit.persistence.PersistentDataType;
import org.bukkit.scheduler.BukkitRunnable;

public class Alcohol extends BukkitRunnable {
    public static NamespacedKey alcLvKey = new NamespacedKey("qol", "alcohol_level");
    public static NamespacedKey alcAmountKey = new NamespacedKey("qol", "alcohol_amount");
    public static NamespacedKey alcPerKey = new NamespacedKey("qol", "alcohol_percent");
    public static NamespacedKey alcKey = new NamespacedKey("qol", "alcohol");
    // PlayerのPersistentDataContainerのAlcoholLevelの値によってプレイや―に毎分効果を与える
    // 0.00~0.02 効果なし
    // 0.02~0.05 プレイヤーの移動速度が5%上昇
    // 0.05~0.10 プレイヤーの移動速度が10%上昇, プレイヤーの攻撃力が5%上昇
    // 0.10~0.15 プレイヤーの移動速度が10%低下
    // 0.15~0.30 プレイヤーの移動速度が15%低下、吐き気が10秒発生
    // 0.30~0.40 プレイヤーの移動速度が20%低下、吐き気が20秒発生、暗闇が10秒発生
    // 0.40~ 即時死亡

    // AlcholLevelの値は毎分0.01ずつ減少する

    @Override
    public void run() {
        for (Player player : Bukkit.getOnlinePlayers()){
            // プレイヤーのAlcoholLevelを取得
            // ない場合は無視
            if (!player.getPersistentDataContainer().has(alcLvKey, PersistentDataType.DOUBLE)) continue;
            double alcLv = player.getPersistentDataContainer().get(alcLvKey, PersistentDataType.DOUBLE);
            // 一旦効果をリセット
            player.setWalkSpeed(0.100f);
            player.getAttribute(org.bukkit.attribute.Attribute.GENERIC_ATTACK_DAMAGE).setBaseValue(1.00);
            // 効果を与える
            // TODO speedを%で指定する方法がわからない
            if (alcLv >= 0.02 && alcLv < 0.05) {
                player.setWalkSpeed(0.105f);
            } else if (alcLv >= 0.05 && alcLv < 0.10) {
                player.setWalkSpeed(0.110f);
                player.getAttribute(org.bukkit.attribute.Attribute.GENERIC_ATTACK_DAMAGE).setBaseValue(1.05);
            } else if (alcLv >= 0.10 && alcLv < 0.15) {
                player.setWalkSpeed(0.090f);
            } else if (alcLv >= 0.15 && alcLv < 0.30) {
                player.setWalkSpeed(0.085f);
                player.addPotionEffect(new org.bukkit.potion.PotionEffect(org.bukkit.potion.PotionEffectType.CONFUSION, 200, 0));
            } else if (alcLv >= 0.30 && alcLv < 0.40) {
                player.setWalkSpeed(0.080f);
                player.addPotionEffect(new org.bukkit.potion.PotionEffect(org.bukkit.potion.PotionEffectType.CONFUSION, 400, 0));
                player.addPotionEffect(new org.bukkit.potion.PotionEffect(org.bukkit.potion.PotionEffectType.BLINDNESS, 200, 0));
            } else if (alcLv >= 0.40) {
                player.damage(1000);
            }

            // 0.01減少
            alcLv -= 0.01;
            // 0.00以下ならalcohol_levelを削除
            if (alcLv <= 0.00) {
                player.getPersistentDataContainer().remove(alcLvKey);
                continue;
            }
            player.getPersistentDataContainer().set(alcLvKey, PersistentDataType.DOUBLE, alcLv);
        }

    }
}