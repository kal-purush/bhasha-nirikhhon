#region © 2022 Aflac.
//
// All rights reserved. Reproduction or transmission in whole or in part, in
// any form or by any means, electronic, mechanical, or otherwise, is prohibited
// without the prior written consent of the copyright owner.
//
#endregion

using System.Windows.Forms;

using JsonToTreeView;

namespace JSON_Formatter
{
    // ----------------------------------------------------
    /// <summary>
    ///     ClassExtensions Description
    /// </summary>

    public static class ClassExtensions
    {
        public static JTree JTree(this TabPage page) { return page.Controls[0] as JTree; }
    }
}