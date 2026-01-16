using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace TodoManager2.model {
    public class Todo {

        public Todo() {

        }

        public Todo(DateTime creationDate) {
            CreationDateTime = creationDate;
        }

        public Todo(DateTime creationDate, DateTime completionDate) : this(creationDate) {
            CompletionDateTime = completionDate;
        }

        public DateTime CreationDateTime { private set; get; } = DateTime.Now;

        public string CreationDateString {
            get {
                return CreationDateTime.ToString();
            }
        }

        public string Title { get; set; }

        public bool IsCompleted {
            get {
                return IsCompleted;
            }
            set {
                IsCompleted = value;
                if (IsCompleted) {
                    if (CompletionDateTime == DateTime.MinValue) {
                        CompletionDateTime = DateTime.Now;
                    }
                }
            }
        }

        public string Text { get; set; }

        public string CompletionComment { get; set; }

        public DateTime CompletionDateTime { get; private set; } = new DateTime();

        public DateTime DueDateTime { get; set; } = new DateTime();

        public TimeSpan Span { get; set; } = new TimeSpan();

    }
}