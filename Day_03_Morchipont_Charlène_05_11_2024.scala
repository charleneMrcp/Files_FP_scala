// Databricks notebook source
// MAGIC %md
// MAGIC # Functions
// MAGIC
// MAGIC Functions are expressions that have: 
// MAGIC - parameters
// MAGIC - take arguments
// MAGIC
// MAGIC **Anonymous functions** 
// MAGIC
// MAGIC You can define an anonymous function ie a function **that has no name**

// COMMAND ----------

(x: Int) => x + 1

// COMMAND ----------

// MAGIC %md
// MAGIC #### Functions with name

// COMMAND ----------

val adOne = (x: Int) => x + 1

// COMMAND ----------

// MAGIC %md
// MAGIC A function can have multiple parameters

// COMMAND ----------

val add = (x: Int, y: Int) => x + y
println(add(1, 2))

// COMMAND ----------

// MAGIC %md
// MAGIC Or it can have no parameters at all

// COMMAND ----------

val getTheAnswer = () => 42
println(getTheAnswer())

// COMMAND ----------

// MAGIC %md
// MAGIC # Methods
// MAGIC
// MAGIC Methods look and behave very similar to functions
// MAGIC
// MAGIC - But they differ on some points:
// MAGIC
// MAGIC - Methods are defined with the **def** keyword
// MAGIC
// MAGIC - **def** is followed by a:
// MAGIC   - name
// MAGIC   - parameter list(s)s
// MAGIC   - a return type, a body

// COMMAND ----------

def add(x: Int, y: Int): Int = x + y
println(add(1, 2))

// COMMAND ----------

// MAGIC %md
// MAGIC The return type **Int** is declared after the parameter list
// MAGIC
// MAGIC *A method can take multiple parameter lists*

// COMMAND ----------

def addThenMultiply(x: Int, y: Int)(multiplier: Int): Int = (x + y)* multiplier
println(addThenMultiply(1, 2)(3)) //9

// COMMAND ----------

// MAGIC %md
// MAGIC Or no parameter lists at all

// COMMAND ----------

def name: String = System.getProperty

// COMMAND ----------

// MAGIC %md
// MAGIC # Classes
// MAGIC
// MAGIC You can define classes with the keyword, followed by its name and constructor parameters:

// COMMAND ----------

class Greeter(prefix: String, suffix: String) {
  def greet(name: String): Unit = 
  println(prefix + name + suffix)
}

// COMMAND ----------

// MAGIC %md
// MAGIC - The return type pf the method greet is Unit, which signifies that there is nothing meaningful to return.
// MAGIC - It is used similarly to void in Java and C.
// MAGIC   -(A difference is that, because every Scala expression must have some value, there is  actually a singleton of )

// COMMAND ----------

val greeter = new Greeter("Hello, ", "!")
greeter.greet("Scala developer") // Hello, Scala developer!

// COMMAND ----------

// MAGIC %md
// MAGIC # Case Classes
// MAGIC
// MAGIC - Scala has special type of classes named "case" classes which are immutable by default cand are compared by value (not by ref like basic classes)
// MAGIC - This make them additionnaly useful for pattern matching
// MAGIC - So no need to use "new" before the creation 
// MAGIC
// MAGIC You can defined them with the keyword : **case** before class

// COMMAND ----------

case class Point(x: Int, y: Int)

// COMMAND ----------

val point = Point(1, 2)
val anotherPoint = Point(1, 2)
val yetAnotherPoint = Point(2, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC Instances of case classes are **compared by value**

// COMMAND ----------

// MAGIC %md
// MAGIC # Objects
// MAGIC
// MAGIC - Objects are 

// COMMAND ----------

object IdFactory {
  private var counter = 0
  def create(): Int = {
    counter += 1
    counter
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC You can acces an object by reffering to its name

// COMMAND ----------

val newId: Int = IdFactory.create()
println(newId)
val newerId: Int = IdFactory.create()
println(newerId)

// COMMAND ----------

// MAGIC %md
// MAGIC # Traits
// MAGIC
// MAGIC - Abstract data types contining certain fields and methods
// MAGIC - In Sclaa inheritance, a class can only extend one other class, but it can extends multiple traits
// MAGIC
// MAGIC You can defined traits with the **trait** keyword

// COMMAND ----------

trait Greeter {
  def greet(name: String): Unit
}

// COMMAND ----------

class DefaultGreeter extends Greeter 

class CustomizableGreeter(prefix: String, suffix: String) extends Greeter {
  override def greet(name: String)
}

// COMMAND ----------

object Main {
  def main(args: Array[String]): Unit = 
  println("Hello, Scala devleoper!")
}
