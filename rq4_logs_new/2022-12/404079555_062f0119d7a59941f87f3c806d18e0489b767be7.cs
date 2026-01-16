using System;
using System.ComponentModel.DataAnnotations;

namespace Rinkudesu.Gateways.Clients.LinkTags;

public class LinkTagDto
{
    public Guid LinkId { get; set; }
    public Guid? TagId { get; set; }
}