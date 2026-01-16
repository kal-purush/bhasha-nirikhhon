//  IMPORTANT:
//  This file is generated. Your changes will be lost.
//  Use the corresponding partial class for customizations.
//  Source Table: [Reclamation].[CostAuthorityObligationRequestPotentialObligationNumberMatch]
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Collections.Generic;
using System.Data.Entity.Spatial;
using System.Linq;
using System.Web;
using CodeFirstStoreFunctions;
using LtInfo.Common;
using LtInfo.Common.DesignByContract;
using LtInfo.Common.Models;

namespace ProjectFirmaModels.Models
{
    // Table [Reclamation].[CostAuthorityObligationRequestPotentialObligationNumberMatch] is NOT multi-tenant, so is attributed as ICanDeleteFull
    [Table("[Reclamation].[CostAuthorityObligationRequestPotentialObligationNumberMatch]")]
    public partial class CostAuthorityObligationRequestPotentialObligationNumberMatch : IHavePrimaryKey, ICanDeleteFull
    {
        /// <summary>
        /// Default Constructor; only used by EF
        /// </summary>
        protected CostAuthorityObligationRequestPotentialObligationNumberMatch()
        {

        }

        /// <summary>
        /// Constructor for building a new object with MaximalConstructor required fields in preparation for insert into database
        /// </summary>
        public CostAuthorityObligationRequestPotentialObligationNumberMatch(int costAuthorityObligationRequestPotentialObligationNumberMatchID, int costAuthorityObligationRequestID, int obligationNumberID) : this()
        {
            this.CostAuthorityObligationRequestPotentialObligationNumberMatchID = costAuthorityObligationRequestPotentialObligationNumberMatchID;
            this.CostAuthorityObligationRequestID = costAuthorityObligationRequestID;
            this.ObligationNumberID = obligationNumberID;
        }

        /// <summary>
        /// Constructor for building a new object with MinimalConstructor required fields in preparation for insert into database
        /// </summary>
        public CostAuthorityObligationRequestPotentialObligationNumberMatch(int costAuthorityObligationRequestID, int obligationNumberID) : this()
        {
            // Mark this as a new object by setting primary key with special value
            this.CostAuthorityObligationRequestPotentialObligationNumberMatchID = ModelObjectHelpers.MakeNextUnsavedPrimaryKeyValue();
            
            this.CostAuthorityObligationRequestID = costAuthorityObligationRequestID;
            this.ObligationNumberID = obligationNumberID;
        }


        /// <summary>
        /// Creates a "blank" object of this type and populates primitives with defaults
        /// </summary>
        public static CostAuthorityObligationRequestPotentialObligationNumberMatch CreateNewBlank()
        {
            return new CostAuthorityObligationRequestPotentialObligationNumberMatch(default(int), default(int));
        }

        /// <summary>
        /// Does this object have any dependent objects? (If it does have dependent objects, these would need to be deleted before this object could be deleted.)
        /// </summary>
        /// <returns></returns>
        public bool HasDependentObjects()
        {
            return false;
        }

        /// <summary>
        /// Active Dependent type names of this object
        /// </summary>
        public List<string> DependentObjectNames() 
        {
            var dependentObjects = new List<string>();
            
            return dependentObjects.Distinct().ToList();
        }

        /// <summary>
        /// Dependent type names of this entity
        /// </summary>
        public static readonly List<string> DependentEntityTypeNames = new List<string> {typeof(CostAuthorityObligationRequestPotentialObligationNumberMatch).Name};


        /// <summary>
        /// Delete just the entity 
        /// </summary>
        public void Delete(DatabaseEntities dbContext)
        {
            dbContext.CostAuthorityObligationRequestPotentialObligationNumberMatches.Remove(this);
        }
        
        /// <summary>
        /// Delete entity plus all children
        /// </summary>
        public void DeleteFull(DatabaseEntities dbContext)
        {
            
            Delete(dbContext);
        }

        [Key]
        public int CostAuthorityObligationRequestPotentialObligationNumberMatchID { get; set; }
        public int CostAuthorityObligationRequestID { get; set; }
        public int ObligationNumberID { get; set; }
        [NotMapped]
        public int PrimaryKey { get { return CostAuthorityObligationRequestPotentialObligationNumberMatchID; } set { CostAuthorityObligationRequestPotentialObligationNumberMatchID = value; } }



        public static class FieldLengths
        {

        }
    }
}