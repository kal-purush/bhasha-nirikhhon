
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FamilyTree {
    private HashMap<Integer, Person> persons;
    private ArrayList<Relation> relations;
    private int id_counter;

    public FamilyTree() {
        persons = new HashMap<>();
        relations = new ArrayList<>();
        id_counter = 0;
    }

    /**
     * Сохранение дерева в файл
     *
     * @param file_path путь к файлу
     */
    public void save(String file_path) {
        try (DataOutputStream ds = new DataOutputStream(new FileOutputStream(file_path))) {
            ds.writeInt(persons.size());
            for (Person it : persons.values()) {
                it.save(ds);
            }
            ds.writeInt(relations.size());
            for (Relation it : relations) {
                it.save(ds);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }

    /**
     * Загрузка дерева из файла
     *
     * @param file_path путь к файлу
     */
    public void load(String file_path) {
        try (DataInputStream ds = new DataInputStream(new FileInputStream(file_path))) {
            persons.clear();
            relations.clear();
            int count = ds.readInt();
            for (int i = 0; i < count; i++) {
                Person person = Person.load(ds);
                persons.put(person.get_id(), person);
            }

            count = ds.readInt();
            for (int i = 0; i < count; i++) {
                Relation relation = Relation.load(ds);
                relations.add(relation);
            }

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Добавление персоны
     *
     * @param name
     * @param family
     * @param middle_name
     * @param gender
     * @return
     */
    public Person addPerson(String name, String family, String middle_name, Person.Gender gender) {
        // #region Получим уникальный ID
        while (persons.containsKey(id_counter))
            id_counter++;
        // #endregion
        Person person = new Person(id_counter, name, family, middle_name, gender);
        persons.put(id_counter, person);
        return person;
    }

    /**
     * Удаление персоны с ID
     *
     * @param id
     * @return true - OK
     */
    public boolean deletePerson(int id) {
        if (persons.containsKey(id) == false)
            return false;
        persons.remove(id);
        return true;
    }

    public Person getPerson(int id) {
        if (persons.containsKey(id) == false)
            return null;
        return persons.get(id);

}

    /**
     * Поиск ID персоны по ФИО
     *
     * @param name
     * @param family
     * @param middle_name
     * @return
     */
    public List<Person> findPersonID(String name, String family, String middle_name) {
        List<Person> res = new ArrayList<>();
        for (Person person : persons.values()) {
            if (person.name.equals(name) && person.family.equals(family) && person.middle_name.equals(middle_name)) {
                res.add(person);
            }
        }
        return res;
    }

    /**
     * Возвращаем список персон
     * @return
     */
    public Collection<Person> getPersons() {
        return persons.values();
    }

    /**
     * Добавление связи между персонами
     *
     * @param relation
     * @return true - OK
     */
    public boolean addRelation(Relation relation) {
        for (Relation it : relations) {
            if (it.equals(relation))
                return true;
            if (it.getID1() == relation.getID1() && it.getID2() == relation.getID2())
                return false; // уже есть связь
            if (it.getID1() == relation.getID2() && it.getID2() == relation.getID1())
                return false; // уже есть связь
        }
        relations.add(relation);
        return true;
    }

    /**
     * Удаление связи меду персонами id1 b id2
     * @param id1
     * @param id2
     * @return
     */
    public boolean deleteRelation(int id1, int id2) {
        for (int i = 0; i < relations.size(); i++) {
            if ((relations.get(i).getID1() == id1 && relations.get(i).getID2() == id2) ||
                    (relations.get(i).getID1() == id2 && relations.get(i).getID2() == id1)) {
                relations.remove(i);
                return true;
            }

        }
        return false;
    }

    /**
     * Возвращаем список связей персон
     *
     * @return
     */
    public ArrayList<Relation> getRelations() {
        return (ArrayList<Relation>) relations.clone(); // возвращаем клон, чтобы обезопаситься от изменения Списка
                                                        // (правда, содержимое все равно передаем по ссылке..)
    }
}