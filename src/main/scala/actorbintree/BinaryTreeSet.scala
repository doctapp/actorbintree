/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o: Operation => root ! o
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! BinaryTreeNode.CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => // Ignore
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case BinaryTreeNode.CopyFinished =>
      context.become(normal)
      root ! PoisonPill
      root = newRoot
      while (pendingQueue.nonEmpty) {
        val (o, q) = pendingQueue.dequeue
        pendingQueue = q
        root ! o
      }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case o @ Insert(requester, id, otherElem) =>
      val r = elem.compareTo(otherElem)
      if (r == 0) {
        removed = false
        requester ! OperationFinished(id)
      } else {
        val pos = if (r < 0) Left else Right
        subtrees.get(pos) match {
          case Some(n) => n ! o
          case None =>
            val node = context.actorOf(BinaryTreeNode.props(otherElem, initiallyRemoved = true))
            subtrees += pos -> node
            node ! o
        }
      }

    case o @ Contains(requester, id, otherElem) =>
      val r = elem.compareTo(otherElem)
      if (r == 0) {
        requester ! ContainsResult(id, !removed)
      } else {
        val pos = if (r < 0) Left else Right
        subtrees.get(pos) match {
          case Some(n) => n ! o
          case None => requester ! ContainsResult(id, false)
        }
      }

    case o @ Remove(requester, id, otherElem) =>
      val r = elem.compareTo(otherElem)
      if (r == 0) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val pos = if (r < 0) Left else Right
        subtrees.get(pos) match {
          case Some(n) => n ! o
          case None => requester ! OperationFinished(id)
        }
      }

    case c @ CopyTo(other) =>
      if (removed && subtrees.isEmpty)
        context.parent ! CopyFinished
      else {
        context.become(copying(subtrees.values.toSet, removed))
        if (!removed)
          other ! Insert(self, elem, elem)
        subtrees.values.foreach(_ ! CopyTo(other))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished =>
      val xs = expected - sender()
      if (xs.isEmpty && insertConfirmed) {
        context.become(normal)
        context.parent ! CopyFinished
      } else {
        context.become(copying(xs, insertConfirmed))
      }

    case OperationFinished(id) =>
      if (expected.isEmpty)
        context.parent ! CopyFinished
      else
        context.become(copying(expected, true))
  }


}
