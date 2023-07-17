package com.labs1904.hwe.exercises

object StretchProblems {

  /*
  Checks if a string is palindrome.
 */
  def isPalindrome(s: String): Boolean = {
    val x = s.replaceAll("[^A-Za-z]+", "").toLowerCase.split("").toList
    if (x == x.reverse) {
      true
    } else {
      false
    }
  }

  /*
For a given number, return the next largest number that can be created by rearranging that number's digits.
If no larger number can be created, return -1
 */
  def getNextBiggestNumber(i: Integer): Int = {
    val max = i.toString.split("").toList.sorted.reverse.mkString("").toInt
    println("i: " + i)
    println("max: " + max)

    if (i == max) {
      -1
    } else {
      val is = i.toString.split("").toList
      val digits = is.length
      //      println("is: " + is)
      //      println("digits: " + digits)
      //      println("")
      val cl = digits match {
        case 2 => max
        case _ => {
          val allVal = is.permutations.toList
            .map(_.foldLeft("")((strVal, curVal) => {
              strVal + curVal
            }
            )).map(_.toInt).sorted
          //          println("x: " + allVal)
          allVal.filter(x => x > i).head
        }
      }
      //      println("cl: " + cl)
      cl
    }
  }
}