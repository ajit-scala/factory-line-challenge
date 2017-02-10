package com.as24.factory

import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ComputationScheduler

import scala.annotation.tailrec

class FactoryService (
  itemSource: FactorySource[Item],
  ordersSource: FactorySource[Order],
  qcService: QCService,
  goodBoxesSink: FactorySink[PresentBox],
  badBoxesSink: FactorySink[PresentBox]
) extends Factory {

  def run(): Unit = {
    val itemsStream = from(itemSource).cache // cache since we want to use the items multiple times but we do not want to consume itemSource multiple times

    val ordersStream = from(ordersSource)

    ordersStream
      .zipWithIndex // since there is no order id we need some identifier to group items otherwise two orders with the same items will be considered equal
      .map { case (Order(items), id) => UniqueOrder(id, items)} // just map to a helper case class to simplify order id handling
      .flatMap(o => itemsStream.map((o, _))) // do cartesian product between orders and items
      .groupBy { case (order, _) => order } // group order-item pairs by order
      .flatMap(boxOrder) // prepare boxes by filtering those order-item pairs
      .groupBy(qcService.isOk) // the rest is obvious
      .flatMap {
        case (true, boxes) =>
          boxes.doOnNext(goodBoxesSink.put)
        case (false, boxes) =>
          boxes.doOnNext(badBoxesSink.put)
      }
      .subscribe()
  }

  /**
    * Helper class to make order identifiable
    */
  case class UniqueOrder(id: Int, items: Seq[Item])

  /**
    * Very interesting function which becomes a stream of pairs (one order -> all possible items) and select those items
    * which were ordered and then puts them to a box.
    */
  private val boxOrder: PartialFunction[(UniqueOrder, Observable[(UniqueOrder, Item)]), Observable[PresentBox]] = {
    case (order, pairs) =>
      pairs.observeOn(ComputationScheduler()) // let's box orders in parallel to make it more interesting :)
        .map(_._2) // we need only incoming items
        .foldLeft(PresentBox(Seq()) -> order.items) {
          case ((box @ PresentBox(boxedItems), missingItems), nextItem) =>
            if (missingItems.contains(nextItem)) {
              val updatedBoxedItems = boxedItems :+ nextItem
              (PresentBox(updatedBoxedItems), missingItems.diff(updatedBoxedItems))
            } else {
              (box, missingItems)
            }
        }
        .map(_._1) // we need only boxes
  }

  private def from[T](source: FactorySource[T]): Observable[T] = Observable { subscriber =>
    @tailrec
    def pull(): Unit = source.pull() match {
      case Some(v) =>
        subscriber.onNext(v)
        pull()
      case None =>
        subscriber.onCompleted()
    }
    pull()
  }
}
