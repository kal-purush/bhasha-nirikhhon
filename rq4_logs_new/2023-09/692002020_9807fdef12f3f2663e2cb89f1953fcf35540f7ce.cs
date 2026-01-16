using CRUD_Demo.Models;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CRUD_Demo
{
    internal class ChoreDataAccess
    {
        //数据库连接字符串
        private const string connectString = "mongodb://127.0.0.1:27017";
        //数据库名称
        private const string databaseName = "dotnet_test_db2";
        //表（collection）名
        private const string collectionStudent = "students";
        private const string collectionChore = "chores";
        private const string collectionChoreHistory = "chores_history";

        //这里的关键字in，限制传入参数的引用，但设置为只读模式。通过关键字in可以提升性能。
        private IMongoCollection<T> ConnectTomongo<T>(in string collection)
        {
            var client=new MongoClient(connectString);
            var db = client.GetDatabase(databaseName);
            return db.GetCollection<T>(collection);
        }

        /// <summary>
        /// 获取所有学生列表
        /// </summary>
        /// <returns></returns>
        internal async Task<List<StudentModel>> GetAllStudents()
        {
            var studentCollection = ConnectTomongo<StudentModel>(collectionStudent);
            var results = await studentCollection.FindAsync(_=>true);
            return results.ToList();    
        }

        /// <summary>
        /// 根据Id获取一个学生
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        internal async Task<StudentModel> GetStudentById(string id)
        {
            var studentCollection = ConnectTomongo<StudentModel>(collectionStudent);
            var result = await studentCollection.FindAsync( s=> s.Id==id);
            var studentList=result.ToList();
            if(studentList.Count>0)
            {
                return studentList[0];
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 获取所有值日活动列表
        /// </summary>
        /// <returns></returns>
        internal async Task<List<ChoreModel>> GetAllChores()
        {
            var choreCollection = ConnectTomongo<ChoreModel>(collectionChore);
            var results=await choreCollection.FindAsync(_=>true); 
            return results.ToList();
        }

        internal async Task<ChoreModel> GetChoreById(string id)
        {
            var choreCollection = ConnectTomongo<ChoreModel>(collectionChore);
            var result=await choreCollection.FindAsync(c=>c.Id==id);
            var choreList=result.ToList();
            if(choreList.Count>0)
            {
                return choreList[0];
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 获取某个学生的值日活动列表
        /// </summary>
        /// <param name="student"></param>
        /// <returns></returns>
        internal async Task<List<ChoreModel>> GetAllChoresByStudent(StudentModel student)
        {
            var choreCollection = ConnectTomongo<ChoreModel>(collectionChore);
            var results = await choreCollection.FindAsync(c => c.AssignedTo.Id==student.Id);
            return results.ToList();
        }

        /// <summary>
        /// 添加一个学生
        /// </summary>
        /// <param name="student"></param>
        /// <returns></returns>
        internal async Task CreateStudent(StudentModel student)
        {
            var studentCollection = ConnectTomongo<StudentModel>(collectionStudent);
            await studentCollection.InsertOneAsync(student);
        }

        /// <summary>
        /// 添加一个值日活动
        /// </summary>
        /// <param name="chore"></param>
        /// <returns></returns>
        internal async Task CreateChore(ChoreModel chore)
        {
            var choresCollection = ConnectTomongo<ChoreModel>(collectionChore);
            await choresCollection.InsertOneAsync(chore);
        }

        /// <summary>
        /// 更新值日活动。
        /// MongoDb的更新与关系型数据库更新不一样，即使只更新一条数据中的某个属性，也会把整条记录更新一遍。（效率是很高的）
        /// </summary>
        /// <param name="chore"></param>
        /// <returns></returns>
        internal async Task UpdateChore(ChoreModel chore)
        {
            var choresCollection=ConnectTomongo<ChoreModel>(collectionChore);
            var filter = Builders<ChoreModel>.Filter.Eq("Id",chore.Id);
            //如果存在则更新，如果不存在则创建。
            ReplaceOptions options = new ReplaceOptions() {IsUpsert=true };
            await choresCollection.ReplaceOneAsync(filter, chore, options);
        }

        /// <summary>
        /// 删除指定的值日活动
        /// </summary>
        /// <param name="chore"></param>
        /// <returns></returns>
        internal async Task DeleteChore(ChoreModel chore)
        {
            var choresCollection = ConnectTomongo<ChoreModel>(collectionChore);
            await choresCollection.DeleteOneAsync(c=>c.Id==chore.Id);
        }

        internal async Task CreateChoreHistory(ChoreModel chore,string updateBy)
        {
            var historyCollection = ConnectTomongo<ChoreHistoryModel>(collectionChoreHistory);
            await historyCollection.InsertOneAsync(new ChoreHistoryModel(chore,updateBy));
        }
    }
}