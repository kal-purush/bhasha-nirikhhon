using Autodesk.Revit.DB;
using Autodesk.Revit.Exceptions;
using Autodesk.Revit.UI;
using Autodesk.Revit.UI.Selection;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace AlignTag
{
  internal class Align
  {
    public Result AlignElements(ExternalCommandData commandData, ref string message, AlignType alignType)
    {
            UIDocument activeUiDocument = commandData.Application.ActiveUIDocument;
            using (TransactionGroup txg = new TransactionGroup(activeUiDocument.Document))
      {
        try
        {
          this.AlignTag(activeUiDocument, alignType, txg);
                    return (Result) 0;
        }
        catch (Autodesk.Revit.Exceptions.OperationCanceledException ex)
        {
          if (txg.HasStarted())
            txg.RollBack();
          return (Result) 1;
        }
        catch (ErrorMessageException ex)
        {
          message = ex.Message;
          if (txg.HasStarted())
            txg.RollBack();
                    return Result.Failed;
        }
        catch (Exception ex)
        {
          message = ex.Message;
          if (txg.HasStarted())
            txg.RollBack();
          return Result.Failed;
                }
      }
    }

    private List<AnnotationElement> RetriveAnnotationElementsFromSelection(UIDocument UIDoc, Transaction tx)
    {
      ICollection<ElementId> elementIds = UIDoc.Selection.GetElementIds();
      List<Element> elementList = new List<Element>();
      List<AnnotationElement> annotationElementList = new List<AnnotationElement>();
      tx.Start("Prepare tags");
      using (IEnumerator<ElementId> enumerator = ((IEnumerable<ElementId>) elementIds).GetEnumerator())
      {
                while (((IEnumerator)enumerator).MoveNext())
                {
                    ElementId current = enumerator.Current;
                    Element element = UIDoc.get_Document().GetElement(current);
                    if (((object)element).GetType() == typeof(IndependentTag))
                    {
                        IndependentTag independentTag = element as IndependentTag;
                        independentTag.set_LeaderEndCondition((LeaderEndCondition)1);
                        if (independentTag.get_HasLeader())
                        {
                            independentTag.set_LeaderEnd(independentTag.get_TagHeadPosition());
                            independentTag.set_LeaderElbow(independentTag.get_TagHeadPosition());
                        }
            elementList.Add(element);
          }
          else if (((object) element).GetType() == typeof (TextNote))
          {
            (element as TextNote).RemoveLeaders();
            elementList.Add(element);
          }
          else if (((object) element).GetType().IsSubclassOf(typeof (SpatialElementTag)))
          {
            SpatialElementTag spatialElementTag = element as SpatialElementTag;
            if (spatialElementTag.get_HasLeader())
            {
              spatialElementTag.set_LeaderEnd(spatialElementTag.get_TagHeadPosition());
              spatialElementTag.set_LeaderElbow(spatialElementTag.get_TagHeadPosition());
            }
            elementList.Add(element);
          }
          else
            elementList.Add(element);
        }
      }
      FailureHandlingOptions failureHandlingOptions = tx.GetFailureHandlingOptions();
      failureHandlingOptions.SetFailuresPreprocessor((IFailuresPreprocessor) new CommitPreprocessor());
      tx.Commit(failureHandlingOptions);
      using (List<Element>.Enumerator enumerator = elementList.GetEnumerator())
      {
        while (enumerator.MoveNext())
        {
          Element current = enumerator.Current;
          annotationElementList.Add(new AnnotationElement(current));
        }
      }
      return annotationElementList;
    }

    private void AlignTag(UIDocument UIDoc, AlignType alignType, TransactionGroup txg)
    {
      Document document = UIDoc.Document();
      ICollection<ElementId> elementIds = UIDoc.Selection().GetElementIds();
      bool flag = false;
      using (Transaction tx = new Transaction(document))
      {
        txg.Start("Align Tag");
        if (elementIds.Count == 0)
        {
          flag = true;
          IList<Reference> selectedReferences = UIDoc.get_Selection().PickObjects((ObjectType) 1, "Pick elements to be aligned");
          elementIds = Tools.RevitReferencesToElementIds(document, selectedReferences);
          UIDoc.get_Selection().SetElementIds(elementIds);
        }
        List<AnnotationElement> source = this.RetriveAnnotationElementsFromSelection(UIDoc, tx);
        txg.RollBack();
        txg.Start("Align Tag");
        tx.Start("Align Tags");
        if (source.Count > 1)
        {
          document.get_ActiveView().get_UpDirection();
          switch (alignType)
          {
            case AlignType.Left:
              AnnotationElement annotationElement1 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpLeft.X())).FirstOrDefault<AnnotationElement>();
              using (List<AnnotationElement>.Enumerator enumerator = source.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(annotationElement1.UpLeft.X(), current.UpLeft.Y(), 0.0);
                  current.MoveTo(point, AlignType.Left);
                }
                break;
              }
            case AlignType.Right:
              AnnotationElement annotationElement2 = source.OrderByDescending<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_X())).FirstOrDefault<AnnotationElement>();
              using (List<AnnotationElement>.Enumerator enumerator = source.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(annotationElement2.UpRight.get_X(), current.UpRight.get_Y(), 0.0);
                  current.MoveTo(point, AlignType.Right);
                }
                break;
              }
            case AlignType.Up:
              AnnotationElement annotationElement3 = source.OrderByDescending<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_Y())).FirstOrDefault<AnnotationElement>();
              using (List<AnnotationElement>.Enumerator enumerator = source.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(current.UpRight.get_X(), annotationElement3.UpRight.get_Y(), 0.0);
                  current.MoveTo(point, AlignType.Up);
                }
                break;
              }
            case AlignType.Down:
              AnnotationElement annotationElement4 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.DownRight.get_Y())).FirstOrDefault<AnnotationElement>();
              using (List<AnnotationElement>.Enumerator enumerator = source.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(current.DownRight.get_X(), annotationElement4.DownRight.get_Y(), 0.0);
                  current.MoveTo(point, AlignType.Down);
                }
                break;
              }
            case AlignType.Center:
              List<AnnotationElement> list1 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_X())).ToList<AnnotationElement>();
              double num1 = (list1.LastOrDefault<AnnotationElement>().Center.get_X() + list1.FirstOrDefault<AnnotationElement>().Center.get_X()) / 2.0;
              using (List<AnnotationElement>.Enumerator enumerator = list1.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(num1, current.Center.get_Y(), 0.0);
                  current.MoveTo(point, AlignType.Center);
                }
                break;
              }
            case AlignType.Middle:
              List<AnnotationElement> list2 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_Y())).ToList<AnnotationElement>();
              double num2 = (list2.LastOrDefault<AnnotationElement>().Center.get_Y() + list2.FirstOrDefault<AnnotationElement>().Center.get_Y()) / 2.0;
              using (List<AnnotationElement>.Enumerator enumerator = list2.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(current.Center.get_X(), num2, 0.0);
                  current.MoveTo(point, AlignType.Middle);
                }
                break;
              }
            case AlignType.Vertically:
              List<AnnotationElement> list3 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_Y())).ToList<AnnotationElement>();
              AnnotationElement annotationElement5 = list3.LastOrDefault<AnnotationElement>();
              AnnotationElement annotationElement6 = list3.FirstOrDefault<AnnotationElement>();
              double num3 = (annotationElement5.Center.get_Y() - annotationElement6.Center.get_Y()) / (double) (source.Count - 1);
              int num4 = 0;
              using (List<AnnotationElement>.Enumerator enumerator = list3.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(current.Center.get_X(), annotationElement6.Center.get_Y() + (double) num4 * num3, 0.0);
                  current.MoveTo(point, AlignType.Vertically);
                  ++num4;
                }
                break;
              }
            case AlignType.Horizontally:
              List<AnnotationElement> list4 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.UpRight.get_X())).ToList<AnnotationElement>();
              AnnotationElement annotationElement7 = list4.LastOrDefault<AnnotationElement>();
              AnnotationElement annotationElement8 = list4.FirstOrDefault<AnnotationElement>();
              double num5 = (annotationElement7.Center.get_X() - annotationElement8.Center.get_X()) / (double) (source.Count - 1);
              int num6 = 0;
              using (List<AnnotationElement>.Enumerator enumerator = list4.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(annotationElement8.Center.get_X() + (double) num6 * num5, current.Center.get_Y(), 0.0);
                  current.MoveTo(point, AlignType.Horizontally);
                  ++num6;
                }
                break;
              }
            case AlignType.UntangleVertically:
              List<AnnotationElement> list5 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (y => y.GetLeaderEnd().get_Y())).ToList<AnnotationElement>();
              AnnotationElement annotationElement9 = list5.FirstOrDefault<AnnotationElement>();
              double num7 = 0.0;
              using (List<AnnotationElement>.Enumerator enumerator = list5.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(current.UpLeft.get_X(), annotationElement9.UpLeft.Y() + num7, 0.0);
                  current.MoveTo(point, AlignType.UntangleVertically);
                  num7 += current.UpLeft.get_Y() - current.DownLeft.get_Y();
                }
                break;
              }
            case AlignType.UntangleHorizontally:
              List<AnnotationElement> list6 = source.OrderBy<AnnotationElement, double>((Func<AnnotationElement, double>) (x => x.GetLeaderEnd().get_X())).ToList<AnnotationElement>();
              AnnotationElement annotationElement10 = list6.FirstOrDefault<AnnotationElement>();
              double num8 = 0.0;
              using (List<AnnotationElement>.Enumerator enumerator = list6.GetEnumerator())
              {
                while (enumerator.MoveNext())
                {
                  AnnotationElement current = enumerator.Current;
                  XYZ point = new XYZ(annotationElement10.UpLeft.get_X() + num8, current.UpLeft.Y(), 0.0);
                  current.MoveTo(point, AlignType.UntangleHorizontally);
                  num8 += current.UpRight.get_X() - current.UpLeft.get_X();
                }
                break;
              }
          }
        }
        tx.Commit();
        txg.Assimilate();
      }
      if (flag)
      {
        List<ElementId> elementIdList = new List<ElementId>();
        elementIdList.Add(ElementId.get_InvalidElementId());
        elementIds = (ICollection<ElementId>) elementIdList;
      }
    UIDoc.get_Selection().SetElementIds(elementIds);
    }
  }
}