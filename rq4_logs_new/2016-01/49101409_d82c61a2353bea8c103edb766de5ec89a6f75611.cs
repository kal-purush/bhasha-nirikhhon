// Author: Adrian Hum
// Project: WPFCommandPrompt/CommandHistory.cs
// 
// Created : 2016-01-06  18:29 
// Modified: 2016-01-06 18:35)

using System;
using System.Collections.Generic;
using System.Linq;

namespace WPFCommandPrompt.CommandHistory {
    /// <summary>
    ///     Manages the command history.
    /// </summary>
    internal class CommandHistory {
        private const string HistoryFileName = "CommandHistory.xml";
        private List<string> _commandHistory = new List<string>();
        private string _currentCommand;
        private int _currentIndex = -1;

        #region Properties

        /// <summary>
        ///     Gets the total number of commands in the history.
        /// </summary>
        public int Count
        {
            get { return _commandHistory.Count; }
        }

        /// <summary>
        ///     Gets or sets the current command.
        /// </summary>
        /// <value>
        ///     The current command.
        /// </value>
        public string CurrentCommand
        {
            get { return _currentCommand; }
            set
            {
                if (!string.IsNullOrEmpty(value))
                {
                    _currentCommand = value;
                }
            }
        }

        /// <summary>
        ///     Gets the command history list.
        /// </summary>
        public List<string> CommandHistoryList
        {
            get { return _commandHistory; }
        }

        /// <summary>
        ///     Gets or sets the current command history index.
        /// </summary>
        /// <value>
        ///     The current index.
        /// </value>
        public int CurrentIndex
        {
            get { return _currentIndex; }
            set { _currentIndex = value; }
        }

        #endregion

        #region Command Control

        /// <summary>
        ///     Adds to history. Skips empty commands.
        /// </summary>
        /// <param name="command">The command to add.</param>
        public void AddCommandToHistory(string command)
        {
            if (!string.IsNullOrEmpty(command))
            {
                _commandHistory.Add(command);
            }
        }

        /// <summary>
        ///     Clears the command history.
        /// </summary>
        public void ClearHistory()
        {
            _commandHistory.Clear();
            _currentIndex = -1;
        }

        /// <summary>
        ///     Gets the next or previous command in the que.
        /// </summary>
        /// <returns></returns>
        private string GetCommand()
        {
            var selected = _currentCommand;

            if (_commandHistory.Count > 0)
            {
                if (_currentIndex >= 0 && _currentIndex < _commandHistory.Count)
                {
                    selected = _commandHistory.ElementAt(_currentIndex);
                }
            }

            return selected;
        }

        /// <summary>
        ///     Gets the command at the selected index number.
        /// </summary>
        /// <param name="indexNumber">The index number.</param>
        /// <returns></returns>
        public string GetCommand(int indexNumber)
        {
            if (indexNumber >= 0 && indexNumber < _commandHistory.Count)
            {
                return _commandHistory.ElementAt(indexNumber);
            }
            return string.Empty;
        }

        /// <summary>
        ///     Gets the previous command.
        /// </summary>
        /// <returns>The previous command.</returns>
        public string GetPrevious()
        {
            _currentIndex--;
            if (_currentIndex < 0)
            {
                _currentIndex = _commandHistory.Count - 1;
            }
            return GetCommand();
        }

        /// <summary>
        ///     Gets the next command.
        /// </summary>
        /// <returns>The next command.</returns>
        public string GetNext()
        {
            _currentIndex++;
            if (_currentIndex >= _commandHistory.Count)
            {
                _currentIndex = 0;
            }
            return GetCommand();
        }

        /// <summary>
        ///     Removes a command from the command history.
        /// </summary>
        /// <param name="indexNumber">The index number of the command to remove.</param>
        public void RemoveFromCommandHistory(int indexNumber)
        {
            if (indexNumber >= 0 && indexNumber < _commandHistory.Count)
            {
                _commandHistory.RemoveAt(indexNumber);
            }
            else
            {
                throw new ArgumentOutOfRangeException("The selected index number was outside of the bounds of the list");
            }
        }

        #endregion

        #region Load/Save Command History

        /// <summary>
        ///     Loads the command history from an xml file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="fileName">Name of the file.</param>
        public void LoadCommandHistory(string path, string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                fileName = HistoryFileName;
            }
            if (!path.EndsWith(@"\\") || !path.EndsWith(@"\"))
            {
                path += "\\";
            }
            _commandHistory = Utility.XmlFileToObject<List<string>>(path + fileName);
        }

        /// <summary>
        ///     Saves the command history to an xml file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="fileName">Name of the file.</param>
        public void SaveCommandHistory(string path, string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                fileName = HistoryFileName;
            }
            if (!path.EndsWith(@"\\") || !path.EndsWith(@"\"))
            {
                path += "\\";
            }
            Utility.ObjectToXMlFile(path + fileName, _commandHistory);
        }

        #endregion
    }
}