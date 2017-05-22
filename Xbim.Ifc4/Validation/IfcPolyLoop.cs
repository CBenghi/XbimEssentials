using System;
using log4net;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using Xbim.Common.Enumerations;
using Xbim.Common.ExpressValidation;
using Xbim.Ifc4.Interfaces;
// ReSharper disable once CheckNamespace
// ReSharper disable InconsistentNaming
namespace Xbim.Ifc4.TopologyResource
{
	public partial class IfcPolyLoop : IExpressValidatable
	{
		public enum IfcPolyLoopClause
		{
			AllPointsSameDim,
		}

		/// <summary>
		/// Tests the express where-clause specified in param 'clause'
		/// </summary>
		/// <param name="clause">The express clause to test</param>
		/// <returns>true if the clause is satisfied.</returns>
		public bool ValidateClause(IfcPolyLoopClause clause) {
			var retVal = false;
			try
			{
				switch (clause)
				{
					case IfcPolyLoopClause.AllPointsSameDim:
						retVal = Functions.SIZEOF(Polygon.Where(Temp => Temp.Dim != Polygon.ItemAt(0).Dim)) == 0;
						break;
				}
			} catch (Exception ex) {
				var Log = LogManager.GetLogger("Xbim.Ifc4.TopologyResource.IfcPolyLoop");
				Log.Error(string.Format("Exception thrown evaluating where-clause 'IfcPolyLoop.{0}' for #{1}.", clause,EntityLabel), ex);
			}
			return retVal;
		}

		public virtual IEnumerable<ValidationResult> Validate()
		{
			if (!ValidateClause(IfcPolyLoopClause.AllPointsSameDim))
				yield return new ValidationResult() { Item = this, IssueSource = "IfcPolyLoop.AllPointsSameDim", IssueType = ValidationFlags.EntityWhereClauses };
		}
	}
}