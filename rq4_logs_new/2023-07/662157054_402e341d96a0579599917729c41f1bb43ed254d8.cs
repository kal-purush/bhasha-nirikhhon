using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace rb.dal.Models;

public partial class UserSkill
{
    [Key]
    public int Id { get; set; }

    public int? UserId { get; set; }

    public int? SkillId { get; set; }

    [ForeignKey("SkillId")]
    [InverseProperty("UserSkills")]
    public virtual Skill? Skill { get; set; }

    [ForeignKey("UserId")]
    [InverseProperty("UserSkills")]
    public virtual User? User { get; set; }
}