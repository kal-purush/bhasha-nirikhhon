using PandoraMVC.Controllers;
using PandoraMVC.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PandoraMVC
{
    public class ActionRecognizer
    {
        public UiAction RecognizeAction(ICRUDModel<GanttDataSource> data)
        {
            if (ActionIsAddEpic(data))
            {
                return UiAction.AddEpic;
            }
            else if (ActionIsAddTask(data))
            {
                return UiAction.AddTask;
            }
            else if (ActionIsEditTask(data))
            {
                return UiAction.EditTask;
            }
            else if (ActionIsEditEpic(data))
            {
                return UiAction.EditEpic;
            }
            else if (ActionIsDeleteTask(data))
            {
                return UiAction.DeleteTask;
            }
            else if (ActionIsDeleteEpic(data))
            {
                return UiAction.DeleteEpic;
            }
            else
            {
                return UiAction.Unknown;
            }
        }

        private bool ActionIsAddEpic(ICRUDModel<GanttDataSource> data)
        {
            return (data.added != null && data.added.Count() > 0)
                && (data.deleted == null || data.deleted.Count() == 0)
                && (data.changed == null || data.changed.Count() == 0);
        }

        private bool ActionIsAddTask(ICRUDModel<GanttDataSource> data)
        {
            return (data.added != null && data.added.Count() > 0)
                && (data.deleted == null || data.deleted.Count() == 0)
                && (data.changed != null && data.changed.Count() > 0);
        }

        private bool ActionIsEditTask(ICRUDModel<GanttDataSource> data)
        {
            return (data.added == null || data.added.Count() == 0)
                && (data.changed != null && data.changed.Count() == 2);
        }

        private bool ActionIsEditEpic(ICRUDModel<GanttDataSource> data)
        {
            return (data.added == null || data.added.Count() == 0)
                && (data.changed != null && data.changed.Count() == 1)
                && (data.deleted == null || data.deleted.Count() == 0)
                && data.changed.First().isEpic == true;
        }

        private bool ActionIsDeleteTask(ICRUDModel<GanttDataSource> data)
        {
            return (data.deleted != null && data.deleted.Count() > 0)
                && data.deleted.First().isEpic == false;
        }

        private bool ActionIsDeleteEpic(ICRUDModel<GanttDataSource> data)
        {
            return (data.deleted != null && data.deleted.Count() > 0)
                && data.deleted.First().isEpic == true;
        }
    }
}