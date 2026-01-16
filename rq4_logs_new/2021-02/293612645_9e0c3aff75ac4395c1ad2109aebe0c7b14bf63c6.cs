// --------------------------------------------------------------------------------------------------------------------
// <copyright file="CreateLogEntryDialogViewModel.cs"company="RHEA System S.A.">
//    Copyright(c) 2021 RHEA System S.A.
// 
//    Author: Sam Gerené, Alex Vorobiev, Alexander van Delft, Nathanael Smiechowski, Ahmed Abulwafa Ahmed
// 
//    This file is part of DEHP Common Library
// 
//    The DEHPCommon is free software; you can redistribute it and/or
//    modify it under the terms of the GNU Lesser General Public
//    License as published by the Free Software Foundation; either
//    version 3 of the License, or (at your option) any later version.
// 
//    The DEHPCommon is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//    Lesser General Public License for more details.
// 
//    You should have received a copy of the GNU Lesser General Public License
//    along with this program; if not, write to the Free Software Foundation,
//    Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace DEHPCommon.UserInterfaces.ViewModels
{
    using ReactiveUI;

    /// <summary>
    /// The view-model for the CreateLogEntryDialog view
    /// </summary>
    public class CreateLogEntryDialogViewModel : ReactiveObject
    {
        /// <summary>
        /// Backing field for the <see cref="LogEntryContent"/> property
        /// </summary>
        private string logEntryContent;

        /// <summary>
        /// Gets or sets the log entry content value
        /// </summary>
        public string LogEntryContent
        {
            get => this.logEntryContent;
            set => this.RaiseAndSetIfChanged(ref this.logEntryContent, value);
        }
    }
}