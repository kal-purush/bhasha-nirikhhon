using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using GalaxyMapSiteApi.Data;
using GalaxyMapSiteApi.Models.Map;

namespace GalaxyMapSiteApi.Models;

[Table("governments")]
public class Government {
    #region Properties
    [Key]
    public string Name { get; set; }
    [NotMapped]
    public MapColor Color { get; set; } = MapColor.Gray;
    public string ColorString {
        get { return Color.ToString(); }
        set { Color = (MapColor)Enum.Parse(typeof(MapColor), value); }
    }
    public virtual ICollection<PlanetGovernment> PlanetGovernments { get; set;} = [];
    [NotMapped]
    public List<Planet> Planets { 
        get { return ((List<PlanetGovernment>)PlanetGovernments).ConvertAll(p => p.Planet); }
    }
    public virtual ICollection<GovernmentGovernment> ParentGovernments { get; set; } = [];
    public Government GetParentGovernment() {
        if (ParentGovernments.Count > 0) {
            return ParentGovernments.First().ParentGovernment.GetParentGovernment();
        }
        return this;
    }
    public Government GetGalacticGovernment() {
        Government parent = GetParentGovernment();
        // @TODO(jaymirecki): replace this comparison with an IEquatable comparison
        if (parent.Name == this.Name){
            return parent;
        }
        return parent.GetGalacticGovernment();
    }
    #endregion Properties
    #region Constructors
    public Government(string name, string colorString) {
        Name = name;
        ColorString = colorString;
    }
    #endregion Constructors
}