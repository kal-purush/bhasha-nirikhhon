using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Xcaciv.Command.Interface.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public class CommandRootAttribute : Attribute
    {
        private string _command = "";
        /// <summary>
        /// define how this command is to be called
        /// </summary>
        /// <param name="command"></param>
        /// <param name="description"></param>
        public CommandRootAttribute(string command = "", string description = "") 
        { 
            this.Command = command;
            this.Description = description;
        }
        /// <summary>
        /// the base command string
        /// </summary>
        /// <example>DIR</example>
        public string Command { 
            get
            { return this._command; }
            set
            { this._command = CommandDescription.GetValidCommandName(value); }
        }
        /// <summary>
        /// What does this command do
        /// </summary>
        public string Description { get; set; } = "TODO";
        /// <summary>
        /// a short name for the command
        /// eg. "ls" for "list"
        /// </summary>
        public string Alias { get; set; } = "";
    }
}