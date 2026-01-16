using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Prism.Mvvm;

namespace TodoManager2.model {
    public class TodoSearchOption : BindableBase{

        private List<Tag> tags = new List<Tag>();
        public List<Tag> Tags {
            get => tags;
            set {
                tags = value;
                RaisePropertyChanged();
            }
        }

        private Boolean tagSearchTypeisOR = false;

        /// <summary>
        /// タグ検索の方式が OR 検索であるかどうかを示します。
        /// false に指定した場合、And 検索に切り替わります。
        /// </summary>
        public Boolean TagSearchTypeIsOR {
            get => tagSearchTypeisOR;
            set {
                tagSearchTypeisOR = value;
                RaisePropertyChanged();
            }
        }

        private DateTime searchStartPoint = DateTime.MinValue;

        /// <summary>
        /// このプロパティに指定した日時以降に作祭されたTodoを検索するよう指定します。
        /// デフォルトでは全期間を検索結果に含めるようになっています。
        /// </summary>
        public DateTime SearchStartPoint {
            get => searchStartPoint;
            set {
                searchStartPoint = value;
                RaisePropertyChanged();
            }
        }

        private Boolean shouldSelectComplitionTodo = true;

        /// <summary>
        /// 完了済みのTodoを検索結果に含めるかどうかを指定します。デフォルトは true です
        /// </summary>
        public Boolean ShouldSelectComplitionTodo {
            get => shouldSelectComplitionTodo;
            set {
                shouldSelectComplitionTodo = value;
                RaisePropertyChanged();
            }
        }

        private Boolean shouldSelectIncompleteTodo = true;

        /// <summary>
        /// 未完了のTodoを検索結果に含めるかどうかを指定します。デフォルトは　true です
        /// </summary>
        public Boolean ShouldSelectIncompleteTodo {
            get => shouldSelectIncompleteTodo;
            set {
                shouldSelectIncompleteTodo = value;
                RaisePropertyChanged();
            }
        }

        private long resultCountLimit = 100;
        public long ResultCountLimit {
            get => resultCountLimit;
            set {
                resultCountLimit = value;
                RaisePropertyChanged();
            }
        }

    }
}