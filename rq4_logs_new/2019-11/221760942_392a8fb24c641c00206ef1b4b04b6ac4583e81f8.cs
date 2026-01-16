using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace Algenic.Data.Models
{
    public class CompilationResult
    {
        public int Id { get; set; }
        public int SolutionId { get; set; }
        public string Output { get; set; }
        public string CpuTime { get; set; }
        public string MemoryUsage { get; set; }
        public bool ExecutionSuccessful { get; set; }

        [ForeignKey(nameof(SolutionId))]
        public virtual Solution Solution { get; set; }
    }
}