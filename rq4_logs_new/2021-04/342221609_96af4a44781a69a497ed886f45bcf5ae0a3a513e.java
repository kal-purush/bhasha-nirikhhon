package com.seventhtill.dbManager;

import com.seventhtill.characterSheet.CharacterBuilder;
import com.seventhtill.characterSheet.CharacterDirector;
import com.seventhtill.characterSheet.CharacterSheet;
import com.seventhtill.characterSheet.DnDCharacter;
import com.seventhtill.common.DamageType;
import com.seventhtill.dndclass.AbstractFactoryDndClass;
import com.seventhtill.dndclass.DnDClass;
import com.seventhtill.dndclass.FactoryProducerClass;
import com.seventhtill.item.armour.Armour;
import com.seventhtill.item.armour.ArmourComposite;
import com.seventhtill.item.armour.Shield;
import com.seventhtill.item.weapon.MartialWeapon;
import com.seventhtill.item.weapon.SimpleWeapon;
import com.seventhtill.item.weapon.Weapon;
import com.seventhtill.item.weapon.WeaponAttackType;
import com.seventhtill.race.AbstractFactory;
import com.seventhtill.race.FactoryProducer;
import com.seventhtill.race.Race;
import com.seventhtill.ui.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Queries {
    /**
     * Connect to the database
     * @return the Connection object
     */
    private static Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:C:/workarea/DnDCharacterManager/db.sqlite";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    //Add an armour to the armour db
    public static int addArmour(DnDCharacterDTO DnDCharacterDTO) {
        String name = DnDCharacterDTO.getArmourName();
        boolean adv = DnDCharacterDTO.isDisadvantage();
        int baseArm = DnDCharacterDTO.getBaseArmour();
        int weight = DnDCharacterDTO.getArmourWeight();
        String sql = "INSERT INTO armour (name, disadvantage, baseArmour, weight) VALUES(?,?,?,?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setBoolean(2, adv);
            pstmt.setInt(3, baseArm);
            pstmt.setInt(4,weight);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        //get the index of the row
        int armourRow = 0;
        sql = "SELECT id FROM armour WHERE name = ? and disadvantage = ? and baseArmour = ? and weight = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setBoolean(2, adv);
            pstmt.setInt(3, baseArm);
            pstmt.setInt(4,weight);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                armourRow = rs.getInt("id");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Added a character's Armour! " + "Armour index: " + armourRow);
        return armourRow;
    }

    //Add a weapon to the weapon db
    public static int addWeapon(DnDCharacterDTO DnDCharacterDTO) {
        int weaponRow = 0;
        int weight = DnDCharacterDTO.getArmourWeight();
        List<String> properties = DnDCharacterDTO.getProperties();

        String listOfProperties = "";
        for(String property : properties) {
            listOfProperties += property + ", ";
        }
        int dmgDie = DnDCharacterDTO.getDamageDie();
        DamageType dmgType = DnDCharacterDTO.getDamageType();
        String name = DnDCharacterDTO.getWeaponName();

        String sql = "INSERT INTO weapon (weight, properties, damageDie, damageType, name) VALUES(?,?,?,?,?)";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1,weight);
            pstmt.setString(2,  listOfProperties);
            pstmt.setInt(3, dmgDie);
            pstmt.setObject(4, dmgType);
            pstmt.setString(5, name);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        //get the index of the row
        sql = "SELECT id FROM weapon WHERE weight = ? and properties = ? and damageDie = ? and damageType = ? and name = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1,weight);
            pstmt.setString(2,  listOfProperties);
            pstmt.setInt(3, dmgDie);
            pstmt.setObject(4, dmgType);
            pstmt.setString(5, name);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                weaponRow = rs.getInt("id");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Added a character's weapon! " + "Weapon index: " + weaponRow);
        return weaponRow;
    }

    //Add a DnD Character to the dndcharacter db
    public static void addDnDCharacter(DnDCharacterDTO DnDCharacterDTO, int armourId, int weaponId) {
        String name = DnDCharacterDTO.getCharacterName();
        Race aRace = DnDCharacterDTO.getCharacterRace();
        DnDClass aClass = DnDCharacterDTO.getCharacterClass();

        String sql = "INSERT INTO dndcharacter (characterName, characterRace, characterClass, characterWeapon, characterArmour) VALUES(?,?,?,?,?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1,name);
            pstmt.setString(2, aRace.getName());
            pstmt.setString(3, aClass.getName());
            pstmt.setInt(4, weaponId);
            pstmt.setInt(5, armourId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Added a character!");
    }

    //Retrieve DnD Characters where the weapon and armour ids match their armour and weapon table corresponding entries
    public static ArrayList<DnDCharacter> retrieveDnDCharacter() {
        ArrayList<DnDCharacter> characters = new ArrayList<>();
        String characterName, characterRace, characterClass, characterWeapon, characterArmour;
        int aWeapon, anArmour, aStrength, aConstitution, aDexterity, anIntelligence, aWisdom, aCharisma;
        String sql = "SELECT * FROM dndcharacter";

        try (Connection conn = connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {
                characterName = rs.getString("characterName");
                characterRace = rs.getString("characterRace");
                characterClass =  rs.getString("characterClass");
                aWeapon = rs.getInt("characterWeapon");
                anArmour = rs.getInt("characterArmour");
                /*aStrength = rs.getInt("strength");
                aConstitution = rs.getInt("constitution");
                aDexterity = rs.getInt("dexterity");
                anIntelligence = rs.getInt("intelligence");
                aWisdom = rs.getInt("wisdom");
                aCharisma =  rs.getInt("charisma");*/

                //invoke builder for character
                CharacterBuilder newCharacter = new CharacterSheet();
                CharacterDirector characterDirector = new CharacterDirector(newCharacter);
                characterDirector.makeCharacter();
                //objects to be filled
                DnDCharacter aCharacter = characterDirector.getCharacter();
                //variables for each component
                ArmourComposite tempArmour = getCharacterArmour(anArmour);
                Weapon tempWeapon = getCharacterWeapon(aWeapon);

                //Split for factory to work correctly
                String[] characterRaceSplit = characterRace.split(" ");
                AbstractFactory raceFactory = FactoryProducer.getFactory(characterRaceSplit[1]); //first index
                characterRace = characterRaceSplit[0] + characterRaceSplit[1];
                Race aRace = raceFactory.getRace(characterRace); //entire string
                System.out.println("Race from factory: " + characterRace);
                System.out.println("Split race: " + characterRaceSplit[0] + characterRaceSplit[1]);

                AbstractFactoryDndClass classFactory = FactoryProducerClass.getFactory(characterClass);
                DnDClass aClass = classFactory.getDndClass(characterClass);
                System.out.println("Class factory: " + characterClass);

                //Create object
                aCharacter.setCharacterName(characterName);
                aCharacter.setCharacterRace(aRace);
                aCharacter.setCharacterClass(aClass);
                aCharacter.setCharacterWeapon(tempWeapon);
                aCharacter.setCharacterArmour(tempArmour);

                //add to list
                characters.add(aCharacter);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return characters;
    }

    public static int getCharacterWeaponId(String name, Race aRace, DnDClass aClass) {
        int weaponId = 0;
        String sql = "SELECT characterWeapon FROM dndcharacter where characterName = ? and characterRace = ? and characterClass = ?";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1,name);
            pstmt.setString(2, aRace.getName());
            pstmt.setString(3, aClass.getName());
            pstmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {
                weaponId = rs.getInt("characterWeapon");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return weaponId;
    }

    public static int getCharacterArmourId(String name, Race aRace, DnDClass aClass) {
        int armourId = 0;
        String sql = "SELECT characterArmour FROM dndcharacter where characterName = ? and dndcharacter.characterRace = ? and dndcharacter.characterClass = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1,name);
            //pstmt.setString(2, aRace.getName());
            //pstmt.setString(3, aClass.getName());
            pstmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = connect();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)){

            // loop through the result set
            while (rs.next()) {
                armourId = rs.getInt("characterArmour");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return armourId;
    }

    private static Weapon getCharacterWeapon(int id) {
        //Weapon helpers
        SimpleMeleeWeaponHelper simpleMeleeHelper = new SimpleMeleeWeaponHelper();
        SimpleRangedWeaponHelper simpleRangedHelper = new SimpleRangedWeaponHelper();
        MartialMeleeWeaponHelper martialMeleeHelper = new MartialMeleeWeaponHelper();
        MartialRangedWeaponHelper martialRangedHelper = new MartialRangedWeaponHelper();
        // First set up arrays of each type of weapon
        ArrayList<Weapon> simpleMeleeWeapons = simpleMeleeHelper.init();
        ArrayList<Weapon> simpleRangedWeapons = simpleRangedHelper.init();
        ArrayList<Weapon> martialMeleeWeapons = martialMeleeHelper.init();
        ArrayList<Weapon> martialRangedWeapons = martialRangedHelper.init();

        String weaponName;
        Weapon aWeapon = null;
        String sql = "SELECT * FROM weapon where weapon.id in (select characterWeapon from dndcharacter where characterWeapon = ?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            pstmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            // loop through the result set
            while (rs.next()) {
                weaponName = rs.getString("name");
                //Add whatever weapon we read
                ArrayList<Weapon> weaponList = (ArrayList<Weapon>) Stream.of(simpleMeleeWeapons, simpleRangedWeapons, martialMeleeWeapons, martialRangedWeapons).flatMap(Collection::stream).collect(Collectors.toList());
                for (Weapon weapon : weaponList) {
                    System.out.println("Weapon name " + weapon.getName() + " Weapon weight: " + weapon.getWeight() + " Weapon properties: " + weapon.getProperties() + " Weapon attacktype: " + weapon.getAttackType());
                    if (weaponName.contains(weapon.getName())) {
                        aWeapon = weapon;
                        System.out.println("Hello fren: " + aWeapon.getName());
                    }
                    System.out.println("Name: " + weapon.getName() + " Weight: " + weapon.getWeight() + " Properties: " + weapon.getProperties() + " Weapon Attack Type: " + weapon.getAttackType());
                }
                break;
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return aWeapon;
    }



    private static ArmourComposite getCharacterArmour(int id) {
        //Armour helpers
        LightArmourHelper lightArmourHelper = new LightArmourHelper();
        MediumArmourHelper mediumArmourHelper = new MediumArmourHelper();
        HeavyArmourHelper heavyArmourHelper = new HeavyArmourHelper();
        // Make lists for these
        ArrayList<Armour> lightArmour = lightArmourHelper.init();
        ArrayList<Armour> mediumArmour = mediumArmourHelper.init();
        ArrayList<Armour> heavyArmour = heavyArmourHelper.init();

        ArmourComposite anArmour = null;
        String armourName;
        int armourAdvantage, armourBase, armourWeight;
        String sql = "SELECT * FROM armour where armour.id in (select characterArmour from dndcharacter where characterArmour = ?)";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1,id);
            pstmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = connect();
             PreparedStatement pstmt  = conn.prepareStatement(sql);
             ResultSet rs    = pstmt.executeQuery(sql)){
            // loop through the result set
            while (rs.next()) {
                armourName = rs.getString("name");
                /*armourAdvantage = rs.getInt("disadvantage");
                armourBase = rs.getInt("baseArmour");
                armourWeight = rs.getInt("weight");*/

                //Add whatever armour we read
                ArrayList<Armour> armourList = (ArrayList<Armour>) Stream.of(lightArmour, mediumArmour, heavyArmour).flatMap(Collection::stream).collect(Collectors.toList());
                armourList.add(new Shield());
                for (Armour armour : armourList) {
                    if (armourName.contains(armour.getName())) {
                        anArmour.add(armour);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return anArmour;
    }

    public static void deleteDnDCharacter(String characterName, int weaponId, int armourId) {
        String sql = "DELETE from dndcharacter WHERE characterName = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, characterName);
            // execute the delete statement
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        deleteArmour(armourId);
        deleteWeapon(weaponId);
    }

    private static void deleteWeapon(int id) {
        String sql = "DELETE from weapon WHERE weapon.id = ?";
        try (Connection conn = connect();
              PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setInt(1,id);
            // execute the delete statement
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void deleteArmour(int id) {
        String sql = "DELETE from armour WHERE armour.id = ?";
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setInt(1,id);
            // execute the delete statement
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Update weapon based on it's id
    public static void updateWeapon(int weight, List<String> list, int dmgDie, DamageType dmgType, String name, int id) {

        String listOfProperties = "";
        for(String property : list) {
            listOfProperties += property + ", ";
        }

        String sql = "UPDATE weapon set weight = ?, "
                + "properties = ?,"
                + "damageDie = ?,"
                + "damageType = ?,"
                + "name = ?"
                +"where id = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setInt(1, weight);
            pstmt.setString(2, listOfProperties);
            pstmt.setInt(3, dmgDie);
            pstmt.setObject(4, dmgType);
            pstmt.setString(5, name);
            pstmt.setInt(6, id);
            // update
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Update armour based on it's id
    public static void updateArmour(String name, boolean disadvantage, int baseArmour, int weight, int id) {
        String sql = "UPDATE armour set name = ?, "
                + "disadvantage = ?,"
                + "baseArmour = ?,"
                + "weight = ?"
                +"where id = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, name);
            pstmt.setBoolean(2, disadvantage);
            pstmt.setInt(3, baseArmour);
            pstmt.setInt(4, weight);
            pstmt.setInt(5, id);
            // update
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void updateDnDCharacterName(String newName, String oldName, Race oldRace, DnDClass oldClass) {
        String sql = "UPDATE dndcharacter set characterName = ? "
                +"where characterName = ?"
                + "and characterClass = ?"
                + "and characterRace = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setString(1, newName);
            pstmt.setString(2, oldName);
            pstmt.setString(3,oldClass.getName());
            pstmt.setString(4, oldRace.getName());
            // update
            pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void updateDnDCharacterRace(Race newRace, String oldName, Race oldRace, DnDClass oldClass) {
        String sql = "UPDATE dndcharacter set characterRace = ? "
                +"where characterName = ?"
                + "and characterClass = ?"
                + "and characterRace = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setObject(1, newRace);
            pstmt.setString(2, oldName);
            pstmt.setString(3,oldClass.getName());
            pstmt.setString(4, oldRace.getName());
            // update
            pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void updateDnDCharacterClass(DnDClass newClass, String oldName, Race oldRace, DnDClass oldClass) {
        String sql = "UPDATE dndcharacter set characterClass = ? "
                +"where characterName = ?"
                + "and characterClass = ?"
                + "and characterRace = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // set the corresponding param
            pstmt.setObject(1, newClass);
            pstmt.setString(2, oldName);
            pstmt.setString(3,oldClass.getName());
            pstmt.setString(4, oldRace.getName());
            // update
            pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



}