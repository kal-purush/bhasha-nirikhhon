using System;
using System.Collections.Generic;

namespace dionizos_backend_app.Models;

public partial class Path
{
    public long Id { get; set; }

    public long EventId { get; set; }

    public string PathStr { get; set; } = null!;

    public virtual Event Event { get; set; } = null!;
}