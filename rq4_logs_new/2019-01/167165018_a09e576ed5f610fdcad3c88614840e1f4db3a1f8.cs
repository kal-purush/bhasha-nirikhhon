using Bootcamp.CRUD.Context;
using Bootcamp.CRUD.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bootcamp.CRUD.Controler
{
    public class ManageTransaction
    {
        public int? count;
        public void MenuTransaction()
        {
            Item item = new Item();
            Transaction transaction = new Transaction();
            MyContext _context = new MyContext();
            TransactionItem transactionItem = new TransactionItem();

            //insert master transaction
            transaction.TransactionDate = DateTimeOffset.Now.LocalDateTime;
            _context.Transactions.Add(transaction);
            _context.SaveChanges();

            //get id transaction last
            var getTransaction = _context.Transactions.Max(x => x.Id);
            var getTransactionDetail = _context.Transactions.Find(getTransaction);

            Console.WriteLine("Transaction Id   : " + getTransaction);
            Console.WriteLine("Transaction Date : " + getTransactionDetail.TransactionDate);

            Console.Write("How many items : ");
            count = Convert.ToInt16(Console.ReadLine());
            if (count == null)
            {
                Console.WriteLine("Please insert cpunt of item that you want to buy");
                Console.Read();
            }
            else
            {
                for (int i = 1; i <= count; i++)
                {
                    Console.Write("Insert id item : ");
                    int? id = Convert.ToInt16(Console.ReadLine());
                    if (id == null)
                    {
                        Console.WriteLine("Please insert id that you want to buy");
                        Console.Read();
                    }
                    else
                    {
                        //Pencarian Id Item
                        var getItem = _context.Items.Find(id);
                        if (getItem == null)
                        {
                            Console.WriteLine("We don't have item wit id : " + id);
                            Console.Read();
                        }
                        else
                        {
                            Console.Write("Insert amount of item : ");
                            var quantity = Convert.ToInt16(Console.ReadLine());
                            if (getItem.Stok < quantity)
                            {
                                Console.Write("Stok is not enough, we have " + getItem.Stok);
                                Console.Read();
                            }
                            else
                            {

                                transactionItem.Transactions = getTransactionDetail;
                                transactionItem.Items = getItem;
                                transactionItem.Quantity = quantity;
                                _context.TransactionItems.Add(transactionItem);
                                _context.SaveChanges();
                            }
                        }

                    }
                }
                var getPrice = _context.TransactionItems.Where(x => x.Transactions.Id == getTransactionDetail.Id).ToList();
                int? total = 0;
                foreach (var proceed in getPrice)
                {
                    total += (proceed.Quantity * proceed.Items.Price);
                }
                Console.WriteLine("Total Price : " + total);
                Console.Write("Balance : ");
                int? balance = Convert.ToInt32(Console.ReadLine());
                Console.Write("Exchange : " + (balance - total));
                Console.Read();


                Console.WriteLine("----------------------------------------------------");
                Console.WriteLine("                  TRRANSACTION ID  " + getTransaction);
                Console.WriteLine(getTransactionDetail.TransactionDate.Date);
                Console.WriteLine(getTransactionDetail.TransactionDate.TimeOfDay);
                Console.WriteLine("----------------------------------------------------");
                Console.WriteLine("Name             Quantity        Price         Total");
                Console.WriteLine("----------------------------------------------------");
                var getDatatoDisplay = _context.TransactionItems.Where(x => x.Transactions.Id == getTransactionDetail.Id).ToList();
                //var getDatatoDisplay = _context.Items.Where(x => x.IsDelete == false).ToList();
                //_context.Transactions.Max(x => x.Id)
                // var getItemid = _context.Items.
                // var getDatatoDisplay = _context.TransactionItems.Find.getTransaction.Where(x => x.IsDelete == false)).ToList();

                foreach (var tampilin in getDatatoDisplay)
                {
                    Console.WriteLine("" + tampilin.Items.Name + "\t\t" + tampilin.Quantity + "\t" + tampilin.Items.Price + "\t" + total);
                }
                Console.WriteLine("----------------------------------------------------");
                Console.WriteLine("Total Price : " + total);
                Console.WriteLine("Balance     : " + balance);
                Console.WriteLine("Exchange    : " + (balance - total));
                Console.WriteLine("----------------------------------------------------");
                Console.ReadLine();
                Console.Read();
                //var getDatatoDisplay = _context.Transactions.Where(x => x.IsDelete == false).ToList();

                //if (getDatatoDisplay.Count == 0)
                //{
                //    Console.Write("No Data in your database");
                //}
                //else
                //{
                //    foreach (var tampilin in getDatatoDisplay)
                //    {
                //        Console.WriteLine("=========================================================");
                //        Console.WriteLine("Id                 :" + tampilin.Id);
                //        Console.WriteLine("Transaction Date   :" + tampilin.TransactionDate.DateTime);
                //        Console.WriteLine("Transaction Hour   :" + tampilin.TransactionDate.Hour);
                //        Console.WriteLine("=========================================================");
                //    }
                //    Console.Write("Total Supplier " + getDatatoDisplay.Count);
                //}
                //Console.Read();
            }

        }


    }
}

            /*Console.WriteLine("================== Transaction Data ==================");
            Console.WriteLine("1. Insert");
            Console.WriteLine("======================================================");
            Console.Write("Your Choice : ");
            int choice = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine("======================================================");
            switch (choice)
            {
                case 1:
                    // ini untuk memasukan nilai Name, JoinDate dan CreateDate di Supplier
                    Console.Write("Insert Date of Transaction : ");
                    transaction.TransactionDate = DateTimeOffset.Now.LocalDateTime;
                    transaction.CreateDate = DateTimeOffset.Now.LocalDateTime;

                    _context.Transactions.Add(transaction);
                    result = _context.SaveChanges();
                    if (result > 0)
                    {
                        Console.WriteLine("Insert Successfully");
                        var getDatatoDisplay = _context.Transactions.Where(x => x.IsDelete == false).ToList();

                        if (getDatatoDisplay.Count == 0)
                        {
                            Console.Write("No Data in your database");
                        }
                        else
                        {
                            foreach (var tampilin in getDatatoDisplay)
                            {
                                Console.WriteLine("=========================================================");
                                Console.WriteLine("Id                 :" + tampilin.Id);
                                Console.WriteLine("Transaction Date   :" + tampilin.TransactionDate);
                                Console.WriteLine("=========================================================");
                            }
                            Console.Write("Total Supplier " + getDatatoDisplay.Count);
                        }
                        Console.Read();
                    }
                    else
                    {
                        Console.Write("Insert Failed");
                        Console.Read();
                    }
                    Console.Read();
                    break;
               /* case 2:
                    Console.Write("Not Ready Yet");
                    break;
                case 3:
                    Console.Write("Not Ready Yet");
                    break;
                case 4:
                    Console.Write("Not Ready Yet");
                    break;
                    
                default:
                    Console.Write("Please try again");
                    break;
        }
        }
    }
}*/