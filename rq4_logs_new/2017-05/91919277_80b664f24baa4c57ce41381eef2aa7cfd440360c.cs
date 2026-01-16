using Shared;
using System.Threading;
using System.Windows.Forms;

namespace StoreClient
{
    class MyThread
    {
        public static DataGridView dataGrid;
        public static IRemBook remBook;

        public static void run()
        {
            remBook = (IRemBook)GetRemote.New(typeof(IRemBook));

            while (true)
            {
                Thread.Sleep(5000);
                update(dataGrid);
            }
        }

        public static void getDataGridView(DataGridView dgv)
        {
            dataGrid = dgv;
        }

        public static DataGridView update(DataGridView dgv)
        {
            int[] keys = remBook.getOrdersId();
            Order order;
            dgv.Rows.Clear();

            foreach (int id in keys)
            {
                order = remBook.getOrder(id);
                dgv.Rows.Add(order.id, order.book.title, order.status, order.requestDate, order.replyDate);
            }

            return dgv;
        }

    }
}