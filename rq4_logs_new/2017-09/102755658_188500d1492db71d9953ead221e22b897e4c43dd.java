package seu.service;

import org.springframework.beans.factory.annotation.Autowired;
import seu.dao.*;
import seu.domain.*;
import seu.domain.Class;

import java.util.List;

public class AdminService {
    @Autowired
    ClassDao classDao;
    @Autowired
    StudentDao studentDao;
    @Autowired
    TeacherDao teacherDao;
    @Autowired
    CommodityDao commodityDao;
    @Autowired
    LibraryDao libraryDao;
    @Autowired
    CourseSelectDao courseSelectDao;
    @Autowired
    CourseDao courseDao;
    @Autowired
    DormitoryDao dormitoryDao;

    //管理员添加学生基本信息
    public int addSchoolRoll(Student student){
    }

    //管理员添加学生班级信息
    public int addStudentClass(Class cla){
    }

    //管理员删除学生基本信息
    public int deleteSchoolRollById(int id){

    }

    //管理员删除学生班级信息
    public int deleteClassByClassId(int id){

    }

    //管理员更改学生基本信息
    public int changeStudentNameByStudentId(Student student){

    }

    public int changeClassIdByStudentId(Student student){

    }

    public int changeDormitoryIdByStudentId(Student student){

    }

    public int changeBalanceByStudentId(Student student){

    }

    public int changePasswordByStudentId(Student student){

    }

    //管理员更改学生班级信息
    public int changeClassByClassId(Class cla){

    }

    //管理员查询某个学生信息
    public Student getStudentByStudentId(int studentId){

    }

    //管理员获取所有学生的信息
    public List<Student> getAllStudents(){

    }

    //管理员通过学号某个学生班级信息
    public Class getClassByStudentId(int id){

    }

    //管理员通过班级号获取班级信息
    public Class getClassByClassId(int id){

    }

    //管理员获取所有学生的班级信息
    public List<Class> getAllClass(){
        return classDao.queryAll();
    }

    //管理员添加老师信息
    public int addTeacher(Teacher teacher){

    }

    //管理员删除老师信息
    public int deleteTeacherByTeacherId(int id){

    }

    //管理员更改老师信息
    public int changeTeacherNameByTeacherId(Teacher teacher){

    }

    public int changePhoneNumberByTeacherId(Teacher teacher){

    }

    public int changePasswordByTeacherId(Teacher teacher){

    }

    //管理员获取某个老师信息
    public Teacher getTeacherByTeacherId(int id){

    }

    //管理员获取所有老师的信息
    public List<Teacher> getAllTeachers(){

    }

    //管理员添加商品信息
    public int addCommodity(Commodity comm) {

    }

    //管理员删除商品信息
    public int deleteCommodityById(int id) {

    }

    //管理员更改商品库存
    public int changeInventoryById(Commodity commodity) {

    }

    //管理员更改商品价格
    public int changePriceById(Commodity commodity) {

    }

    //管理员获取某个商品信息
    public List<Commodity> getCommodityById(int id) {

    }

    //管理员获取所有商品的信息
    public List<Commodity> getAllCommodity() {

    }

    //管理员添加课程
    public int insertCourse(Course course) {

    }

    //管理员删除课程
    public int deleteCourseByCourseId(int id) {

    }

    //管理员更新课程
    public int updateCourseByCourseId(Course course) {

    }

    //管理员获取课程的全部信息
    public List<Course> getCourseAll() {

    }

    //管理员增加宿舍
    public int insertDormitory(Dormitory dormitory) {

    }

    //管理员删除宿舍
    public int deleteDormitoryByID(int id) {

    }

    //管理员更新学生分数与宿舍信息
    public int updateScoreByID(Dormitory dormitory) {

    }

    //管理员查询所有学生成绩及宿舍信息
    public List<Dormitory> getDormitoryAll() {

    }

    //管理员添加图书
    public int insertBook(Library book){

    }

    //管理员删除图书
    public int deleteBookByBookId(int id) {

    }

    //管理员更新图书信息
    public int updateBookByBookId(Library book) {

    }

    //管理员获取所有学生所借图书全部信息
    public List<Library> getLibraryAll() {

    }
}