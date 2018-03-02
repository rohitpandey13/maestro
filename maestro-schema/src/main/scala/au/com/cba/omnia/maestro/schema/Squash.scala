//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
package au.com.cba.omnia.maestro.schema

import scala.collection._


/** Histogram squasher */
object Squash {

  /** Given a histogram of how many values in a column match each classifier,
   *  remove the counts that don't provide any extra type information.
   *
   *   We do three types of squashing:
   *
   *  1) Squash Parents.
   *     If we have a child count classifier that has the same count as the parent,
   *     then we remove the count for the parent.
   *
   *     As parent classifiers are guaranteed to match all values that their
   *     children do, the count for the parent doesn't tell use anything
   *     more than the count for the child.
   *
   *     For example, if we have Real:100 and Digits:100 we only keep
   *     Digits:100 because digit strings are also real numbers.
   *
   *  2) Squash Partitioned.
   *     For some parents node with count N, if there are child nodes that are
   *     partitions of the parent, all the partitions have counts, and the sum
   *     of those counts sum to N, then we can remove *all* child nodes reachable
   *     from the parent.
   *
   *     For example, if we have Real:100, PosReal:70 and NegReal:30, we only keep
   *     Real:100. The fact that a certain percentage of values are Positive
   *     or Negative is useful information in itself, but we don't need it when
   *     deciding what type the values are. All we want is the name of the set
   *     that contains all the values in the column (ie Real).
   *
   *  3) Squash Separate. (TODO: this case subsumes Squash Parents above.)
   *     For some parent node with count N, if there is a set of child nodes
   *     that are mutually separate, and the the sum of the counts for the child
   *     nodes is N, then remove the count for the parent.
   *
   *     For example, if we have Digits:100, Exact("0"):40, Exact("1"):60,
   *     then we only keep  Exact("0"):40 and Exact("1"):60.
   *
   *     As parent classifiers are guaranteed to match all values that their
   *     children do, then the count of the parent doesn't tell us anything
   *     more than the count for the child.
   */
  def squash(hist: Histogram): Histogram = {

    // Squash all the things.
    val squashSet =
      squashParents(hist) ++
      squashPartitioned(hist) ++
      squashSeparate(hist)

    // Remove all the unneeded classifiers in one go.
    val histS = Histogram(hist.counts.filterKeys { k => ! squashSet.contains (k) })

    // If we're making progress, then keep squashing.
    if (hist.size == histS.size) hist
    else squash(histS)
  }


  // ----------------------------------------------------------------
  /** If we have a child count classifier that has the same count as the parent,
      then we remove the count for the parent. */
  def squashParents(hist: Histogram): Set[Classifier] =
    hist.counts
      .flatMap { tChild => squashSetOfParent(hist, tChild._1) }
      .toSet


  /** Collect the set of parent classifiers to remove,
      given a single child classifier. */
  def squashSetOfParent(hist: Histogram, child: Classifier): Set[Classifier] =
    child match {
      case ClasSyntax(sChild) =>
        sChild.parents
          .flatMap { sParent => squashSetOfParentChild(hist, ClasSyntax(sParent), child) }
          .toSet

      case ClasTope  (_, sChild) =>
        sChild.parents
          .flatMap { sParent => squashSetOfParentChild(hist, ClasSyntax(sParent), child) }
          .toSet
      }


  /** Collect the set of parent classifiers to remove,
      given a single parent-child relationship. */
  def squashSetOfParentChild(
      hist: Histogram, parent: Classifier, child: Classifier): Set[Classifier] =
    if   (hist.counts.isDefinedAt(parent) && hist.counts.isDefinedAt(child)) {
      if (hist.counts(parent) == hist.counts(child))
            Set(parent)
      else  Set()
    }
    else Set()


  // ----------------------------------------------------------------
  /** For some parents node with count N, if there are child nodes that are
   *  partitions of the parent, all the partitions have counts, and the sum
   *  of those counts sum to N, then we can remove *all* child nodes reachable
   *  from the parent. */
  def squashPartitioned(hist: Histogram): Set[Classifier] =
    hist.counts
      .flatMap { tNode => squashPartitionedOfParent(hist, tNode._1) }
      .toSet

  /** When squashing partitions, get the children to remove given a
      single parent classifier. */
  def squashPartitionedOfParent(hist: Histogram, master: Classifier): Set[Classifier] =
    master match {
      case ClasTope(_, _) =>
        Set.empty

      case ClasSyntax(sMaster) => {

        // All the children of this master node.
        val ssChildren: Set[Syntax] =
          Syntaxes.children(sMaster)

        // The children that are partitions of the master.
        val ssPartitions: Set[Syntax] =
          Syntaxes.partitions(sMaster)

        val csPartitions: Set[(Syntax, Int)] =
          ssPartitions.flatMap { s =>
            if (hist.counts.isDefinedAt(ClasSyntax(s)))
                  Seq((s, hist.counts(ClasSyntax(s))))
            else  Seq((s, 0))
          }

        // Sum of counts of for all the partition nodes.
        val countPartitions =
          csPartitions.toSeq.map   { cn => cn._2 }.sum

        // The total number of partitions.
        val nPartitions =
          csPartitions.size

        // Number of partition nodes that have counts associated with them.
        val nPartitionsActive =
          csPartitions.filter { cn => cn._2 > 0}.size

        // Count for the master node.
        val countMaster =
          hist.counts.getOrElse(master, 0)

        // All the desendents of the master node.
        val ssDescendants: Set[Syntax] =
          Syntaxes.descendants(sMaster)

        // If partitions have the same counts of the master then,
        // we want to remove all descendants.
        if ((nPartitions > 0)                &&
            (countMaster == countPartitions) &&
            (nPartitions == nPartitionsActive))
            ssDescendants.map { s => ClasSyntax(s) }.toSet
        else  Set.empty
      }
    }


  // ----------------------------------------------------------------
  /** For some parent node with count N, if there is a set of child nodes that
   *  are mutually separate, and the sum of all those nodes is N, then remove
   *  the parent. */
  def squashSeparate(hist: Histogram): Set[Classifier] =
    hist.counts
      .flatMap { tNode => squashSeparateOfParent(hist, tNode._1) }
      .toSet

  /** When squashing separate, get the classifiers to remove given the
      starting parent classifier. */
  def squashSeparateOfParent(hist: Histogram, parent: Classifier): Set[Classifier] =
    parent match {
      case ClasTope(_, _) =>
        Set.empty

      case ClasSyntax(sParent) => {

        // All the children of the master node.
        val ssDescendants: Set[Syntax] =
          Syntaxes.descendants(sParent)

        val histS: Map[Syntax, Int] =
          hist.counts.map {
            case (ClasSyntax(s),  n)  => (s, n)
            case (ClasTope(_, s), n)  => (s, n) }

        // The descendants along with their counts.
        val snDescendants: Set[(Syntax, Int)] =
          ssDescendants.flatMap { s =>
            if (histS.isDefinedAt(s))
                  Seq((s, histS(s)))
            else  Seq((s, 0)) }
            .toSet

        // The active descendants.
        val snDescendantsActive: Set[(Syntax, Int)] =
          snDescendants.filter { sn => sn._2 > 0 }.toSet

        // Sum of counts of all the active descendants.
        val countDescendants: Int =
          snDescendantsActive.map { sn => sn._2 }.sum

        // Flags saying whether each active descendant is separate from all the others.
        val fsSeparate: Set[Boolean] =
          snDescendantsActive.flatMap { sn1 =>
          snDescendantsActive.map     { sn2 =>
            if   (sn1 == sn2) true
            else (Syntaxes.separate(sn1._1, sn2._1))
          }}

        // Count for the parent node.
        val countParent =
          if (hist.counts.isDefinedAt(parent))
                hist.counts(parent)
          else  0

        // If there is at least one child, and all the children are separate,
        // and the counts on the children sum to the counts on the parent,
        // then we can remove the parent.
        if ( (snDescendantsActive.size > 0) &&
             (fsSeparate       == Set(true)) &&
             (countDescendants == countParent))
             Set(parent)
        else Set()
      }
    }
}
