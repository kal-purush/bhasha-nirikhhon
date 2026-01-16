using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DrAnalyzer.Tree
{
    public class TreeContainer
    {
        private readonly List<TreeNode> childrenNodes = new List<TreeNode>();

        public System.Windows.Forms.TreeNode AddPath(string filePath)
        {
            string currPath = filePath;
            List<string> FullPath = new List<string>();
            FullPath.Insert(0, System.IO.Path.GetFileName(currPath));
            currPath = System.IO.Path.GetPathRoot(currPath);
            while (System.IO.Path.IsPathRooted(currPath))
            {
                FullPath.Insert(0, System.IO.Path.GetDirectoryName(currPath));
                currPath = System.IO.Path.GetPathRoot(currPath);
            }
            Queue<string> FullPathQueue = new Queue<string>(FullPath);

            TreeNode treeNode = childrenNodes.Find(val => val.Name == FullPathQueue.First());
            
            if (treeNode = childrenNodes)
        }
    }
}